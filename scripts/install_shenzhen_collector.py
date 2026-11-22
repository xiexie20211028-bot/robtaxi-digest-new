"""在当前 Mac 用户下安装深圳交通单源采集，每天本地时间 08:15 执行。"""
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys

from bs4 import BeautifulSoup  # 检查定时任务使用的解释器已安装解析依赖。


def main():
    root = Path(__file__).resolve().parents[1]
    label = "com.robtaxi.shenzhen-collector"
    base = Path.home() / "Library" / "Application Support" / label
    gh = shutil.which("gh")
    if not gh:
        raise SystemExit("请先安装并认证 GitHub CLI")
    subprocess.run([gh, "auth", "status", "--active", "--hostname", "github.com"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    base.mkdir(parents=True, exist_ok=True)
    app = base / "app"
    app.mkdir(exist_ok=True)
    for name in ("__init__.py", "shenzhen_snapshot.py", "fetch_structured.py", "site_rules.py", "common.py"):
        shutil.copy2(root / "app" / name, app / name)
    shutil.copy2(root / "sources.json", base / "sources.json")
    plist = Path.home() / "Library" / "LaunchAgents" / (label + ".plist")
    plist.parent.mkdir(parents=True, exist_ok=True)
    plist.write_bytes(plistlib.dumps({
        "Label": label, "ProgramArguments": [sys.executable, "-m", "app.shenzhen_snapshot"],
        "WorkingDirectory": str(base), "StartCalendarInterval": {"Hour": 8, "Minute": 15},
        "EnvironmentVariables": {"PATH": str(Path(gh).parent) + ":/usr/bin:/bin:/usr/sbin:/sbin"},
        "StandardOutPath": str(base / "collector.log"), "StandardErrorPath": str(base / "collector.err"),
    }))
    domain = f"gui/{os.getuid()}"
    if subprocess.run(["launchctl", "print", domain + "/" + label],
                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        subprocess.run(["launchctl", "bootout", domain + "/" + label], check=True)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist)], check=True)
    print(f"已安装 {plist}")


if __name__ == "__main__":
    main()
