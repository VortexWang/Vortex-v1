// vortexctl 是后端运维命令（BE-T10 部分）。
//
//	vortexctl ca init -dir DIR [-org NAME]            生成根 CA 与中间 CA（中间 CA 私钥以 $VORTEX_CA_PASSPHRASE 加密）
//	vortexctl token hub -config FILE -region R [-ttl 24h]   签发 Edge Hub 引导令牌
//	vortexctl token node -config FILE -created-by UUID [-tenant UUID]  签发节点注册令牌（30 分钟）
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vortex/backend/internal/fleet"
	"vortex/backend/internal/pki"
	"vortex/backend/internal/platform"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func parseUUID(s string) (platform.UUID, error) {
	var u platform.UUID
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		return u, fmt.Errorf("invalid uuid %q", s)
	}
	copy(u[:], b)
	return u, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprintln(stderr, "usage: vortexctl ca init | token hub | token node")
		return 2
	}
	switch args[0] + " " + args[1] {
	case "ca init":
		fs := flag.NewFlagSet("ca init", flag.ContinueOnError)
		fs.SetOutput(stderr)
		dir := fs.String("dir", "", "output directory")
		org := fs.String("org", "VORTEX", "organization name")
		if fs.Parse(args[2:]) != nil || *dir == "" {
			return 2
		}
		pass := os.Getenv("VORTEX_CA_PASSPHRASE")
		if err := caInit(*dir, *org, []byte(pass)); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintf(stdout, "CA written to %s; move root.key offline (SEC-012)\n", *dir)
		return 0
	case "token hub", "token node":
		fs := flag.NewFlagSet(args[1], flag.ContinueOnError)
		fs.SetOutput(stderr)
		cfgPath := fs.String("config", "/etc/vortex/backend.json", "backend config")
		region := fs.String("region", "", "region (hub token)")
		ttl := fs.Duration("ttl", 24*time.Hour, "validity (hub token)")
		createdBy := fs.String("created-by", "", "admin user id (node token)")
		tenant := fs.String("tenant", "", "tenant id for a self-hosted node (node token)")
		if fs.Parse(args[2:]) != nil {
			return 2
		}
		cfg, err := platform.LoadConfig(*cfgPath)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		ctx := context.Background()
		pool, err := platform.OpenDB(ctx, cfg.DatabaseURL)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		defer pool.Close()
		svc := &fleet.Service{DB: pool}
		var tok string
		if args[1] == "hub" {
			tok, err = svc.CreateHubBootstrapToken(ctx, *region, *ttl)
		} else {
			var by, tn platform.UUID
			if by, err = parseUUID(*createdBy); err == nil && *tenant != "" {
				tn, err = parseUUID(*tenant)
			}
			if err == nil {
				tok, err = svc.CreateEnrollToken(ctx, tn, by)
			}
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintln(stdout, tok)
		return 0
	}
	fmt.Fprintf(stderr, "unknown command %q\n", strings.Join(args[:2], " "))
	return 2
}

func caInit(dir, org string, pass []byte) error {
	g, err := pki.Generate(org)
	if err != nil {
		return err
	}
	enc, err := pki.EncryptKey(g.CA.IntermediateKey(), pass)
	if err != nil {
		return err
	}
	rk, err := pki.EncryptKey(g.RootKey, pass)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for name, b := range map[string][]byte{"root.crt": pki.CertPEM(g.CA.Root), "intermediate.crt": pki.CertPEM(g.CA.Inter),
		"intermediate.key": enc, "root.key": rk} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return fmt.Errorf("%s exists; refusing to overwrite", name)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			return err
		}
	}
	return nil
}
