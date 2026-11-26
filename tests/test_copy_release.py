from typer.testing import CliRunner

from accli import cli


def test_release_flag_rejected_on_download(tmp_path):
    result = CliRunner().invoke(
        cli.app,
        ["copy", "acc://demo/data", str(tmp_path / "out"), "--release"],
    )
    assert result.exit_code == 1
    assert "--release is only supported when uploading" in result.output
