package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"Scanner/p2p"
)

// ClearCameraWithTunnel очищает текст, подложки и квадраты через уже открытый P2P-туннель.
func ClearCameraWithTunnel(ctx context.Context, tunnel *p2p.PTCPTunnel, login, pass string) error {
	if tunnel == nil {
		return fmt.Errorf("туннель не инициализирован (nil)")
	}

	// 1. Пробуем порт 5000 (DHIP)
	if err := clearDHIP(ctx, tunnel, 5000, login, pass); err == nil {
		return nil
	} else {
		log.Printf("Порт 5000 не сработал (%v), пробуем порт 80 (RPC2)...", err)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 2. Пробуем порт 80 (RPC2)
	return clearRPC2(ctx, tunnel, login, pass)
}

// --- Очистка через DHIP (порт 5000) ---
func clearDHIP(ctx context.Context, tunnel *p2p.PTCPTunnel, port int, login, pass string) error {
	dhip, err := tunnel.NewDHIPClientOnPort(port)
	if err != nil {
		return err
	}
	defer dhip.Close()

	stopDhip := context.AfterFunc(ctx, func() {
		dhip.Close()
	})
	defer stopDhip()

	if err := dhip.LoginNormal(login, pass); err != nil {
		return err
	}

	vw, err := getDHIPTable(dhip, "VideoWidget", 10)
	if err != nil {
		return err
	}

	ct, err := getDHIPTable(dhip, "ChannelTitle", 11)
	if err != nil {
		return err
	}

	ctOut, vwOut := buildCleanTables(ct, vw)
	sess := dhip.Session()

	calls := []any{
		map[string]any{
			"method":  "configManager.setConfig",
			"params":  map[string]any{"name": "ChannelTitle", "table": ctOut, "options": []any{}},
			"id":      41,
			"session": sess,
		},
		map[string]any{
			"method":  "configManager.setConfig",
			"params":  map[string]any{"name": "VideoWidget", "table": vwOut, "options": []any{}},
			"id":      42,
			"session": sess,
		},
	}

	r, err := dhip.Call("system.multicall", calls, 40, nil, nil)
	if err != nil {
		return err
	}
	if ok, _ := r["result"].(bool); !ok {
		return fmt.Errorf("multicall failed: %v", r["error"])
	}
	return nil
}

func getDHIPTable(dhip *p2p.DHIPClient, name string, id int) ([]any, error) {
	r, err := dhip.Call("configManager.getConfig", map[string]any{"name": name}, id, nil, nil)
	if err != nil {
		return nil, err
	}
	params, _ := r["params"].(map[string]any)
	if t, ok := params["table"].([]any); ok {
		return t, nil
	}
	if t, ok := params["table"].(map[string]any); ok {
		return []any{t}, nil
	}
	return nil, fmt.Errorf("bad table for %s", name)
}

// --- Очистка через RPC2 (порт 80) ---
func clearRPC2(ctx context.Context, tunnel *p2p.PTCPTunnel, login, pass string) error {
	realm := rand.Uint32()
	if err := tunnel.DoBindToPort(realm, 80); err != nil {
		return err
	}
	defer tunnel.DisconnectRealm(realm)

	post := func(path string, payload map[string]any, cookie string) (map[string]any, string, error) {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		raw, _ := json.Marshal(payload)
		req := fmt.Sprintf("POST %s HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: %d\r\n", path, len(raw))
		if cookie != "" {
			req += fmt.Sprintf("Cookie: %s\r\n", cookie)
		}
		req += "\r\n" + string(raw)

		resp, err := tunnel.DoHTTPOnRealm(realm, []byte(req), 15*time.Second)
		if err != nil {
			return nil, "", err
		}
		idx := bytes.Index(resp, []byte("\r\n\r\n"))
		if idx < 0 {
			return nil, "", fmt.Errorf("bad http")
		}
		var out map[string]any
		json.Unmarshal(resp[idx+4:], &out)
		return out, string(resp[:idx]), nil
	}

	r1, headers, err := post("/RPC2_Login", map[string]any{
		"method":  "global.login",
		"params":  map[string]any{"userName": login, "password": "", "clientType": "Web3.0", "loginType": "Direct"},
		"id":      1,
		"session": 0,
	}, "")
	if err != nil {
		return err
	}

	var cookie string
	for _, line := range strings.Split(headers, "\r\n") {
		if strings.HasPrefix(strings.ToLower(line), "set-cookie:") && strings.Contains(line, "DhWebClientSessionID=") {
			cookie = strings.TrimSpace(strings.Split(line[11:], ";")[0])
		}
	}

	params, _ := r1["params"].(map[string]any)
	pRealm, _ := params["realm"].(string)
	pRand, _ := params["random"].(string)

	r2, _, err := post("/RPC2_Login", map[string]any{
		"method": "global.login",
		"params": map[string]any{
			"userName":      login,
			"password":      p2p.StandardRPCHash(login, pass, pRealm, pRand),
			"clientType":    "Web3.0",
			"loginType":     "Direct",
			"authorityType": "Default",
		},
		"id":      2,
		"session": r1["session"],
	}, cookie)
	if err != nil || r2["result"] != true {
		return fmt.Errorf("rpc2 login failed")
	}

	sess := r2["session"]
	getTbl := func(name string, id int) []any {
		res, _, _ := post("/RPC2", map[string]any{
			"method":  "configManager.getConfig",
			"params":  map[string]any{"name": name},
			"id":      id,
			"session": sess,
		}, cookie)
		p, _ := res["params"].(map[string]any)
		if t, ok := p["table"].([]any); ok {
			return t
		}
		if t, ok := p["table"].(map[string]any); ok {
			return []any{t}
		}
		return nil
	}

	ctOut, vwOut := buildCleanTables(getTbl("ChannelTitle", 11), getTbl("VideoWidget", 10))
	calls := []any{
		map[string]any{
			"method":  "configManager.setConfig",
			"params":  map[string]any{"name": "ChannelTitle", "table": ctOut, "options": []any{}},
			"id":      41,
			"session": sess,
		},
		map[string]any{
			"method":  "configManager.setConfig",
			"params":  map[string]any{"name": "VideoWidget", "table": vwOut, "options": []any{}},
			"id":      42,
			"session": sess,
		},
	}

	res, _, err := post("/RPC2", map[string]any{
		"method":  "system.multicall",
		"params":  calls,
		"id":      40,
		"session": sess,
	}, cookie)
	if err != nil || res["result"] != true {
		return fmt.Errorf("rpc2 multicall failed")
	}
	return nil
}

// --- Формирование чистых таблиц ---
func buildCleanTables(ctTable, vwTable []any) ([]any, []any) {
	var ctOut []any
	for _, raw := range ctTable {
		if _, ok := raw.(map[string]any); ok {
			ctOut = append(ctOut, map[string]any{"Name": ""})
		}
	}

	var vwOut []any
	for _, raw := range vwTable {
		m, ok := raw.(map[string]any)
		if !ok {
			vwOut = append(vwOut, raw)
			continue
		}

		if w, ok := m["ChannelTitle"].(map[string]any); ok {
			w["EncodeBlend"] = false
			w["PreviewBlend"] = false
			w["BackColor"] = []any{0, 0, 0, 0}
		}

		if covers, ok := m["Covers"].([]any); ok {
			for _, c := range covers {
				if cm, ok := c.(map[string]any); ok {
					cm["EncodeBlend"] = false
					cm["PreviewBlend"] = false
				}
			}
		}

		if ct, ok := m["CustomTitle"].([]any); ok {
			for _, sraw := range ct {
				if sm, ok := sraw.(map[string]any); ok {
					sm["Text"] = ""
					sm["EncodeBlend"] = false
					sm["PreviewBlend"] = false
					sm["BackColor"] = []any{0, 0, 0, 0}
				}
			}
		}

		vwOut = append(vwOut, m)
	}
	return ctOut, vwOut
}
