package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

type Account struct {
	Token string `json:"token"`
	File  string `json:"-"`
}

func loadUsers(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var users []string
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if line != "" {
			users = append(users, line)
		}
		if err == io.EOF {
			break
		}
	}
	return users, nil
}

func loadAccounts(dir string) ([]Account, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Account
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		p := filepath.Join(dir, e.Name())
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var a Account
		if err := json.Unmarshal(b, &a); err != nil {
			var m map[string]interface{}
			if err := json.Unmarshal(b, &m); err == nil {
				if v, ok := m["token"].(string); ok {
					a.Token = v
				} else if v, ok := m["Token"].(string); ok {
					a.Token = v
				}
			}
		}
		if a.Token == "" {
			continue
		}
		a.File = p
		out = append(out, a)
	}
	return out, nil
}

func moveToClaims(accountFile string) error {
	dir := filepath.Dir(accountFile)
	claims := filepath.Join(dir, "claims")
	if err := os.MkdirAll(claims, 0o755); err != nil {
		return err
	}
	base := filepath.Base(accountFile)
	dest := filepath.Join(claims, base)
	return os.Rename(accountFile, dest)
}

func removeUserFromFile(username string) error {
	path := "users.txt"
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(b), "\n")
	var out []string
	for _, l := range lines {
		if strings.TrimSpace(l) == "" {
			continue
		}
		if strings.TrimSpace(l) == username {
			continue
		}
		out = append(out, l)
	}
	return os.WriteFile(path, []byte(strings.Join(out, "\n")+"\n"), 0o644)
}

var totalRequests uint64
var totalClaims uint64
var lastCount uint64
var lastTime time.Time

type Job struct {
	Acc      Account
	Username string
}

type Success struct {
	Message  string
	Username string
	AccFile  string
}

const workerPoolSize = 2000
const maxIdleConnSeconds = 120

func worker(ctx context.Context, client *fasthttp.Client, jobs <-chan Job, success chan<- Success) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobs:
			atomic.AddUint64(&totalRequests, 1)
			payload := map[string]interface{}{
				"operationName": "EditSettingsMutation",
				"variables": map[string]interface{}{
					"input": map[string]interface{}{
						"pronouns": []string{},
						"username": job.Username,
					},
				},
			}
			body, _ := json.Marshal(payload)
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			req.SetRequestURI("https://api.pinterest.com/graphql/")
			req.Header.SetMethod("POST")
			req.Header.Set("Authorization", job.Acc.Token)
			req.Header.Set("X-Pinterest-InstallId", "191f9cd6ce8849ad8278d4d903923b4")
			req.Header.Set("X-Pinterest-Query-Hash", "8b2205bd4818891f85b45f8cba489b3c9a875dd85e967ceef9195041e575f425")
			req.Header.Set("Host", "api.pinterest.com")
			req.Header.Set("Accept", "multipart/mixed;deferSpec=20220824, application/json")
			req.Header.Set("Content-Type", "application/json")
			req.SetBody(body)
			start := time.Now()
			if err := client.Do(req, resp); err != nil {
				fasthttp.ReleaseRequest(req)
				fasthttp.ReleaseResponse(resp)
				continue
			}
			bodyBytes := resp.Body()
			bodyText := string(bodyBytes)

			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
			if strings.Contains(bodyText, "\"username\":\""+job.Username+"\"") {
				moved := false
				if err := moveToClaims(job.Acc.File); err == nil {
					moved = true
				}
				_ = removeUserFromFile(job.Username)
				atomic.AddUint64(&totalClaims, 1)
				base := filepath.Base(job.Acc.File)
				if moved {
					success <- Success{Message: fmt.Sprintf("claimed account %s -> @%s (%vms)", base, job.Username, time.Since(start).Milliseconds()), Username: job.Username, AccFile: job.Acc.File}
				} else {
					success <- Success{Message: fmt.Sprintf("claimed account %s -> @%s but move failed (%vms)", base, job.Username, time.Since(start).Milliseconds()), Username: job.Username, AccFile: job.Acc.File}
				}
			}
		}
	}
}

func main() {
	users, err := loadUsers("users.txt")
	if err != nil || len(users) == 0 {
		fmt.Fprintln(os.Stderr, "no users in users.txt or failed to read")
		return
	}
	accounts, err := loadAccounts("account")
	if err != nil || len(accounts) == 0 {
		fmt.Fprintln(os.Stderr, "no accounts in account/ or failed to read")
		return
	}
	hostClient := &fasthttp.Client{
		NoDefaultUserAgentHeader:      true,
		MaxConnsPerHost:               8000,
		ReadBufferSize:                32 * 4096,
		WriteBufferSize:               12 * 4096,
		ReadTimeout:                   3 * time.Second,
		WriteTimeout:                  3 * time.Second,
		MaxIdleConnDuration:           maxIdleConnSeconds * time.Second,
		DisableHeaderNamesNormalizing: true,
		TLSConfig:                     &tls.Config{InsecureSkipVerify: true, MaxVersion: 0},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs := make(chan Job, workerPoolSize*32)
	success := make(chan Success, 128)
	var usersMu sync.Mutex
	for i := 0; i < workerPoolSize; i++ {
		go worker(ctx, hostClient, jobs, success)
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		lastTime = time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				requests := atomic.LoadUint64(&totalRequests)
				claims := atomic.LoadUint64(&totalClaims)
				elapsed := time.Since(lastTime).Seconds()
				var rps uint64
				if elapsed > 0 {
					rps = uint64(float64(requests-lastCount) / elapsed)
				}
				cPerUser := 0.0
				usersMu.Lock()
				ulen := len(users)
				usersMu.Unlock()
				if ulen > 0 {
					cPerUser = float64(rps) / float64(ulen)
				}
				fmt.Printf("\rRequests: %d / r/s: %d / c/s: %.2f / claims: %d", requests, rps, cPerUser, claims)
				lastCount = requests
				lastTime = time.Now()
			}
		}
	}()

	go func() {
		for s := range success {
			fmt.Println(s.Message)
			usersMu.Lock()
			var remaining []string
			for _, u := range users {
				if u != s.Username {
					remaining = append(remaining, u)
				}
			}
			users = remaining
			usersMu.Unlock()
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			usersMu.Lock()
			us := make([]string, len(users))
			copy(us, users)
			usersMu.Unlock()

			active := false
			for _, acc := range accounts {
				if _, err := os.Stat(acc.File); err != nil {
					continue
				}
				active = true
				for _, u := range us {
					select {
					case <-ctx.Done():
						return
					case jobs <- Job{Acc: acc, Username: u}:
					}
				}
			}
			if !active {
				fmt.Println("\nno accounts left, exiting")
				cancel()
				return
			}
		}
	}()

	<-ctx.Done()
}
