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


def test_disabled_workspace_skill_is_not_listed_or_loaded(tmp_path: Path):
    workspace = tmp_path / "workspace"
    skill_dir = workspace / "skills" / "custom-skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: custom-skill\ndescription: custom\n---\n\nUse custom.",
        encoding="utf-8",
    )

    loader = SkillsLoader(
        workspace,
        builtin_skills_dir=tmp_path / "empty-builtin",
        disabled_skill_names={"custom-skill"},
    )

    assert loader.list_skills(filter_unavailable=False) == []
    assert loader.build_skills_summary() == ""
    assert loader.load_skills_for_context(["custom-skill"]) == ""
