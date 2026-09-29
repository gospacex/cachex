package cachex

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gospacex/configx"
)

// cachex 是对齐其余 x 系列 SDK 补上的三件套成员之一：此前本包只有
// ReadAndCreate / MustReadAndCreate，缺"只读配置不建实例"的 ReadConfig，
// 使 cachex 成为按统一入口调用时会编译失败的例外。
func TestReadConfig_LoadsWithoutCreatingInstances(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte("cachex:\n  instances: []\n"), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	if err := ReadConfig(configx.ConfigOptions{ConfigFile: p}); err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}

	// 关键语义：ReadConfig 不建实例，因此 instances 为空也不会报错
	// （ReadAndCreate 同样输入会以 "no instances configured" 失败）。
	if _, err := ReadAndCreate(configx.ConfigOptions{ConfigFile: p}); err == nil {
		t.Fatal("ReadAndCreate with empty instances must fail — ReadConfig must not have created anything")
	}
}
