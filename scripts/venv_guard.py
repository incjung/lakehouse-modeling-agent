"""
venv_guard.py — 가상환경(venv) 강제 실행 가드 (공통 모듈)

모든 스크립트 최상단에서 호출됩니다:
    from scripts.venv_guard import ensure_venv
    ensure_venv()

가상환경 밖에서 실행되면 즉시 오류를 출력하고 종료합니다.
"""

import os
import sys
from pathlib import Path


def _is_in_venv() -> bool:
    """현재 Python 인터프리터가 venv 안에서 실행 중인지 확인"""
    # sys.prefix != sys.base_prefix → venv 활성화 상태
    in_venv = sys.prefix != sys.base_prefix

    # VIRTUAL_ENV 환경변수 추가 확인 (일부 CI 환경 대응)
    has_env_var = os.environ.get("VIRTUAL_ENV") is not None

    return in_venv or has_env_var


def _find_project_root() -> Path:
    """이 파일 위치에서 프로젝트 루트(setup.sh 있는 곳)를 탐색"""
    current = Path(__file__).resolve().parent
    for parent in [current, *current.parents]:
        if (parent / "setup.sh").exists() or (parent / "requirements.txt").exists():
            return parent
    return current.parent  # 폴백: scripts/ 의 부모


def ensure_venv() -> None:
    """
    venv 활성화 여부를 검사하고, 비활성화 상태이면 사용 방법을 출력 후 종료.

    사용법:
        from scripts.venv_guard import ensure_venv
        ensure_venv()
    """
    if _is_in_venv():
        return  # ✅ 정상: venv 안에서 실행 중

    project_root = _find_project_root()
    venv_dir = ".venv"

    # venv가 아직 생성되지 않은 경우와 생성된 경우를 구분
    venv_path = project_root / venv_dir
    if not venv_path.exists():
        setup_hint = (
            f"  1. 먼저 가상환경을 설정하세요:\n"
            f"       cd {project_root}\n"
            f"       bash setup.sh\n"
            f"\n"
            f"  2. 가상환경을 활성화하세요:\n"
            f"       source {venv_dir}/bin/activate\n"
        )
    else:
        setup_hint = (
            f"  가상환경을 활성화하세요:\n"
            f"       cd {project_root}\n"
            f"       source {venv_dir}/bin/activate\n"
        )

    script_name = Path(sys.argv[0]).name

    print(
        f"\n"
        f"{'='*60}\n"
        f"❌ 오류: 가상환경(venv) 밖에서 실행할 수 없습니다.\n"
        f"{'='*60}\n"
        f"\n"
        f"  현재 Python: {sys.executable}\n"
        f"  스크립트:    {script_name}\n"
        f"\n"
        f"{setup_hint}"
        f"\n"
        f"  활성화 후 다시 실행하세요:\n"
        f"       python scripts/{script_name}\n"
        f"{'='*60}\n",
        file=sys.stderr,
    )
    sys.exit(1)
