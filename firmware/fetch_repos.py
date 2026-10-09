import os
import subprocess
import json


def _apply_patch(path, patch_full_path):
    """单个补丁：先 --check 再 apply。返回是否成功。"""
    check_result = subprocess.run(
        ["git", "-C", path, "apply", "--check", patch_full_path]
    )
    if check_result.returncode == 0:
        subprocess.run(["git", "-C", path, "apply", patch_full_path], check=True)
        print(f"Applied patch {patch_full_path} to {path}")
        return True
    print(f"Patch {patch_full_path} cannot be applied cleanly to {path}, skipped.")
    return False


def clone_or_update_repo(
    repo_url, path, ref=None, with_submodules=False, patch_path=None, patches=None
):
    import os

    if not os.path.exists(path):
        subprocess.run(["git", "clone", repo_url, path], check=True)
    else:
        subprocess.run(["git", "-C", path, "fetch"], check=True)

    if ref:
        subprocess.run(["git", "-C", path, "checkout", ref], check=True)

    if with_submodules:
        subprocess.run(
            ["git", "-C", path, "submodule", "update", "--init", "--recursive"],
            check=True,
        )

    # 应用 patch（按顺序）。patches 列表用于"基线补丁 + 后续移植补丁"的叠加，
    # 例如 xiaozhi-esp32: 官方 m5stack 适配补丁 → notify 流式播报移植补丁。
    patch_list = []
    if patch_path:
        patch_list.append(patch_path)
    if patches:
        patch_list.extend(patches)
    for p in patch_list:
        patch_full_path = p if os.path.isabs(p) else os.path.join(os.getcwd(), p)
        _apply_patch(path, patch_full_path)


def fetch_dependencies():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "repos.json")

    with open(config_path) as f:
        repos = json.load(f)

    for repo in repos:
        repo_path = os.path.join(script_dir, repo["path"])
        branch = repo.get("branch")
        with_submodules = repo.get("with_submodules", False)
        patch = repo.get("patch")
        if patch and not os.path.isabs(patch):
            patch = os.path.join(script_dir, patch)
        patches = [
            p if os.path.isabs(p) else os.path.join(script_dir, p)
            for p in repo.get("patches", [])
        ]
        clone_or_update_repo(
            repo["url"], repo_path, branch, with_submodules, patch, patches
        )


if __name__ == "__main__":
    fetch_dependencies()