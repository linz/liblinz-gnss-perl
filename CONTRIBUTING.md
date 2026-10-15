# Contributing

See [README.md](README.md) for how to build and test.

## Making changes

- Changes go to `master` through pull requests, from a branch named for the change (for example `feat-configurable-http-auth-hosts`, `fix-handle-fails-per-datacenter`).
- Commit and PR titles use a Conventional Commits prefix, for example `feat:`, `fix:`, `refactor:`, `ci:`, `docs:`.
- When adding a runtime dependency, add it to both `requires` in `Build.PL` and `Depends` in `debian/control`.
- `.gitignore` ignores `test/`, but the files in it are tracked. Add new test files with `git add -f`.
- If a change intentionally alters log output, update the matching file in `test/check/`.
- CI runs only a smoke test (see the README). It does not run `test/run_tests.sh`, so run that locally if your change affects data center behaviour.

## Releases

A release is a git tag on this repository. The tag is the version.

### Tag format

Tags look like `1.115.0-1`: `MAJOR.MINOR.PATCH-1`. They are annotated tags whose message is
`Version <tag>`, for example:

```
tag 1.115.0-1
tagger Chris Crook <ccrook@linz.govt.nz> ...

Version 1.115.0-1
```

Recent tags point at commits on `master`, usually the merge commit of a pull request. For
example `1.115.0-1` is the merge of #16 and `1.114.0-1` is the merge of #15.

### Creating a release

1. Merge the pull request to `master`.
2. Create an annotated tag on the commit to release, using the next version number:

   ```sh
   git tag -a 1.116.0-1 -m "Version 1.116.0-1" <commit>
   git push origin 1.116.0-1
   ```

   (`1.116.0-1` is only an example. Check the latest tag with
   `git tag --sort=-creatordate | head -1`.)

### How a release is consumed

Downstream LINZ builds pin a specific tag and install it from the GitHub tag archive
(`https://github.com/linz/liblinz-gnss-perl/archive/refs/tags/<tag>.zip`), then run
`perl -I . Build.PL` and `./Build install` in the unpacked directory. A new tag is not used
by those builds until they update the tag they pin.

### Things to know

- No GitHub Releases are published for this repository, only tags.
- The latest entry in `debian/changelog` is `1.97.0-1` (September 2023). Tags have continued since, up to `1.115.0-1`, without further `debian/changelog` entries.
- `$VERSION` in `lib/LINZ/GNSS.pm` is `1.0.2`. It does not follow the tags.
