Usage: interactive variable updater (embedded in `test.py`)

Files: test.py (contains helpers + interactive updater)

Run:

1) From workspace root run:

```powershell
python test.py
```

2) Choose option 2 to run the Interactive updater.

Flow:
- Input Unit (default `Unit 1`), Boiler (default `Boiler A`), IDF (default `IDF-A`).
- Select analysis by number from the listed analyses.
- The script finds the associated AnalysisRule and reads its `ConfigString`.
- It extracts `VariableN := expression;` blocks and lists them.
- Select a variable (by number or name), then input the new expression (multi-line; finish with an empty line).
- Script shows a unified diff (dry-run). If you confirm `y`, it creates a backup entry in `analysisrule_backups.json` and PATCHes the AnalysisRule.

Notes & safety:
- Default behavior is to show diff only; you must explicitly confirm to apply.
- Backups are stored in `analysisrule_backups.json` with timestamp, webid, old/new config.
- The script uses NTLM auth embedded in `test.py`. Do not commit credentials to shared repos.

If you want, I can now run `python test.py` and step through the interactive flow in dry-run mode for IDF-A.
