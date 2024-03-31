from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

import nox

DIR = Path(__file__).parent.resolve()

nox.options.sessions = ["lint", "pylint", "tests"]  # Session run by default


@nox.session
def lint(session: nox.Session) -> None:
    """
    Run the linter.
    """
    session.install("pre-commit")
    session.run(
        "pre-commit", "run", "--all-files", "--show-diff-on-failure", *session.posargs
    )


@nox.session
def pylint(session: nox.Session) -> None:
    """
    Run PyLint.
    """
    # This needs to be installed into the package environment, and is slower
    # than a pre-commit check
    session.install(".", "pylint")
    session.run("pylint", "idc_index_data", *session.posargs)


@nox.session
def tests(session: nox.Session) -> None:
    """
    Run the unit and regular tests.
    """
    session.install(".[test]")
    session.run("pytest", *session.posargs)


@nox.session(reuse_venv=True)
def docs(session: nox.Session) -> None:
    """
    Build the docs. Pass "--serve" to serve. Pass "-b linkcheck" to check links.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true", help="Serve after building")
    parser.add_argument(
        "-b", dest="builder", default="html", help="Build target (default: html)"
    )
    args, posargs = parser.parse_known_args(session.posargs)

    if args.builder != "html" and args.serve:
        session.error("Must not specify non-HTML builder with --serve")

    extra_installs = ["sphinx-autobuild"] if args.serve else []

    session.install("-e.[docs]", *extra_installs)
    session.chdir("docs")

    if args.builder == "linkcheck":
        session.run(
            "sphinx-build", "-b", "linkcheck", ".", "_build/linkcheck", *posargs
        )
        return

    shared_args = (
        "-n",  # nitpicky mode
        "-T",  # full tracebacks
        f"-b={args.builder}",
        ".",
        f"_build/{args.builder}",
        *posargs,
    )

    if args.serve:
        session.run("sphinx-autobuild", *shared_args)
    else:
        session.run("sphinx-build", "--keep-going", *shared_args)


@nox.session
def build_api_docs(session: nox.Session) -> None:
    """
    Build (regenerate) API docs.
    """

    session.install("sphinx")
    session.chdir("docs")
    session.run(
        "sphinx-apidoc",
        "-o",
        "api/",
        "--module-first",
        "--no-toc",
        "--force",
        "../src/idc_index_data",
    )


@nox.session
def build(session: nox.Session) -> None:
    """
    Build an SDist and wheel.
    """

    build_path = DIR.joinpath("build")
    if build_path.exists():
        shutil.rmtree(build_path)

    session.install("build")
    session.run("python", "-m", "build")


def _bump(session: nox.Session, name: str, script: str, files) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--commit", action="store_true", help="Make a branch and commit."
    )
    parser.add_argument(
        "version", nargs="?", help="The version to process - leave off for latest."
    )
    args = parser.parse_args(session.posargs)

    session.install("db-dtypes")
    session.install("google-cloud-bigquery")
    session.install("pandas")
    session.install("pyarrow")

    if args.version is None:
        gcp_project = "idc-external-025"
        idc_index_version = session.run(
            "python",
            "scripts/python/idc_index_data_manager.py",
            "--project",
            gcp_project,
            "--retrieve-latest-idc-release-version",
            external=True,
            silent=True,
        ).strip()

    else:
        idc_index_version = args.version

    extra = ["--quiet"] if args.commit else []
    session.run("python", script, idc_index_version, *extra)

    if args.commit:
        session.run(
            "git",
            "switch",
            "-c",
            f"update-to-{name.replace(' ', '-').lower()}-{idc_index_version}",
            external=True,
        )
        session.run("git", "add", "-u", *files, external=True)
        session.run(
            "git",
            "commit",
            "-m",
            f"Update to {name} {idc_index_version}",
            external=True,
        )
        session.log(
            f'Complete! Now run: gh pr create --fill --body "Created by running `nox -s {session.name} -- --commit`"'
        )


@nox.session
def bump(session: nox.Session) -> None:
    """
    Set to a new IDC index version, use -- <version>, otherwise will use the latest version.
    """
    files = (
        "pyproject.toml",
        "scripts/sql/idc_index.sql",
        "tests/test_package.py",
    )
    _bump(
        session,
        "IDC index",
        "scripts/python/update_idc_index_version.py",
        files,
    )


@nox.session(venv_backend="none")
def tag_release(session: nox.Session) -> None:
    """
    Print instructions for tagging a release and pushing it to GitHub.
    """

    session.log("Run the following commands to make a release:")
    txt = Path("pyproject.toml").read_text()
    current_version = next(iter(re.finditer(r'^version = "([\d\.]+)$"', txt))).group(1)
    print(
        f"git tag --sign -m 'idc-index-data {current_version}' {current_version} main"
    )
    print(f"git push origin {current_version}")
