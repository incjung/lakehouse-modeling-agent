#!/usr/bin/env bash
# =============================================================
# Lakehouse Modeling Agent — 가상환경(venv) 초기 설정 스크립트
# 사용법: bash setup.sh
# =============================================================
set -euo pipefail

VENV_DIR=".venv"
PYTHON_MIN_MAJOR=3
PYTHON_MIN_MINOR=10
REQUIREMENTS="requirements.txt"

# ── 색상 출력 헬퍼 ─────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()  { echo -e "${CYAN}ℹ️  $*${NC}"; }
ok()    { echo -e "${GREEN}✅ $*${NC}"; }
warn()  { echo -e "${YELLOW}⚠️  $*${NC}"; }
error() { echo -e "${RED}❌ $*${NC}"; exit 1; }

echo -e "${BOLD}"
echo "╔══════════════════════════════════════════════════╗"
echo "║   Lakehouse Modeling Agent — 환경 설정           ║"
echo "╚══════════════════════════════════════════════════╝"
echo -e "${NC}"

# ── 1. Python 버전 확인 ────────────────────────────────────
PYTHON_BIN=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        VER=$("$cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        MAJOR=$(echo "$VER" | cut -d. -f1)
        MINOR=$(echo "$VER" | cut -d. -f2)
        if [ "$MAJOR" -ge "$PYTHON_MIN_MAJOR" ] && [ "$MINOR" -ge "$PYTHON_MIN_MINOR" ]; then
            PYTHON_BIN="$cmd"
            ok "Python $VER 감지: $cmd"
            break
        else
            warn "Python $VER 는 지원하지 않습니다 (최소 ${PYTHON_MIN_MAJOR}.${PYTHON_MIN_MINOR} 필요)"
        fi
    fi
done

[ -z "$PYTHON_BIN" ] && error "Python ${PYTHON_MIN_MAJOR}.${PYTHON_MIN_MINOR}+ 을 찾을 수 없습니다. Python을 먼저 설치해주세요."

# ── 2. venv 생성 ───────────────────────────────────────────
if [ -d "$VENV_DIR" ]; then
    warn "기존 가상환경이 존재합니다: $VENV_DIR"
    read -r -p "  재생성하시겠습니까? (y/N) " REPLY
    if [[ "$REPLY" =~ ^[Yy]$ ]]; then
        info "기존 가상환경 삭제 중..."
        rm -rf "$VENV_DIR"
    else
        info "기존 가상환경을 유지합니다."
    fi
fi

if [ ! -d "$VENV_DIR" ]; then
    info "가상환경 생성 중: $VENV_DIR"
    "$PYTHON_BIN" -m venv "$VENV_DIR"
    ok "가상환경 생성 완료"
fi

# ── 3. pip 업그레이드 + 의존성 설치 ───────────────────────
info "pip 업그레이드 중..."
"$VENV_DIR/bin/python" -m pip install --upgrade pip -q

if [ ! -f "$REQUIREMENTS" ]; then
    error "$REQUIREMENTS 파일을 찾을 수 없습니다."
fi

info "의존성 설치 중: $REQUIREMENTS"
"$VENV_DIR/bin/pip" install -r "$REQUIREMENTS" -q
ok "의존성 설치 완료"

# ── 4. 설치 검증 ───────────────────────────────────────────
info "설치된 패키지 검증 중..."
PACKAGES=("pandas" "pyarrow" "duckdb")
for pkg in "${PACKAGES[@]}"; do
    VER=$("$VENV_DIR/bin/python" -c "import $pkg; print($pkg.__version__)" 2>/dev/null || echo "MISSING")
    if [ "$VER" == "MISSING" ]; then
        error "$pkg 설치 실패"
    else
        ok "$pkg $VER"
    fi
done

# ── 5. activate 안내 ──────────────────────────────────────
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}🎉 설정 완료! 아래 명령으로 가상환경을 활성화하세요:${NC}"
echo ""
echo -e "    ${BOLD}source $VENV_DIR/bin/activate${NC}"
echo ""
echo -e "  스크립트 실행 예시:"
echo -e "    ${CYAN}python scripts/profile_source.py --input data.csv${NC}"
echo -e "    ${CYAN}python scripts/sandbox_validate.py --config design_config.json${NC}"
echo ""
echo -e "  가상환경 종료:"
echo -e "    ${BOLD}deactivate${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
