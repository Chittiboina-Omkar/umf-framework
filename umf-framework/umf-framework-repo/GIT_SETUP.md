# Pushing this project to GitHub

This folder is a complete, ready-to-commit project. To put it on GitHub:

## 1. Create a repository on GitHub

Create a new **private** repository (recommended, since the code is
proprietary), for example `umf-framework`. Do not initialise it with a README,
.gitignore or licence — this folder already has them.

## 2. Initialise git locally and push

From inside this folder:

```bash
git init
git add .
git commit -m "Initial commit: Universal Migration & Data Framework"
git branch -M main
git remote add origin git@github.com:<your-org>/umf-framework.git
git push -u origin main
```

(Use the HTTPS URL `https://github.com/<your-org>/umf-framework.git` instead of
the SSH one if you prefer.)

## 3. Recommended repository settings

- Keep the repository **private**. Distribute only the built wheel to clients,
  via a private package feed (see `docs/DEPLOYMENT.md`), never the source.
- Protect the `main` branch: require a pull request and at least one approval
  before merging. This is how prod changes are reviewed.
- The included CI workflow (`.github/workflows/ci.yml`) runs the test suite on
  every push and pull request automatically.

## 4. Edit before first release

- `LICENSE` — replace the placeholder with your legal entity and chosen licence.
- `pyproject.toml` — set the `[project].name`, `authors`, and version.
- `README.md` — adjust the install index URL to your private feed.

## What is in here

```
umf/                 the framework package (84 modules)
tests/               36 tests (pytest, or python tests/run_tests.py)
configs/             example JSON configs
docs/                deployment, capability matrix, master design, diagrams
pyproject.toml       wheel packaging
.github/workflows/   CI
.gitignore LICENSE CONTRIBUTING.md README.md PROJECT_STRUCTURE.md
```
