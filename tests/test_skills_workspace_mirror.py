from __future__ import annotations

from pathlib import Path

from agent.skills import SkillsLoader


def test_builtin_skills_are_mirrored_into_workspace_summary(tmp_path: Path):
    builtin = tmp_path / "builtin"
    skill_dir = builtin / "demo-skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\ndescription: demo\n---\n\nUse demo.",
        encoding="utf-8",
    )
    workspace = tmp_path / "workspace"

    loader = SkillsLoader(workspace, builtin_skills_dir=builtin)
    summary = loader.build_skills_summary()

    mirrored = workspace / "skills" / "demo-skill" / "SKILL.md"
    assert mirrored.exists()
    assert f"<location>{mirrored}</location>" in summary
    assert str(skill_dir / "SKILL.md") not in summary
