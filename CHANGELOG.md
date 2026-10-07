# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### v1.2.0

- :tada: Add a `capabilities.json` manifest so CloudTAK can read the task's requirements from the image. It declares a single required permission, `feature:submit` (the only CloudTAK API the task uses is `submit()`), 1024 MB memory / 120 s timeout, and a default `rate(5 minutes)` schedule. The schedule is a judgement call for a single small JSON fetch, not a measured camera refresh interval. The manifest is validated against `StaticCapabilitiesSchema` from `@tak-ps/etl`, and a test guards it in CI
- :rocket: Build and push the image with `docker buildx` in the demo and production deploy jobs, embedding `capabilities.json` as the `com.cloudtak.capabilities` OCI annotation, with `docker/setup-buildx-action@v4` providing the `docker-container` builder the annotation needs. Not yet run in the demo environment, so the annotation has not been checked on a pushed image
- :pencil2: Deliberately NOT adopting the `cloudtak-etl` CLI from `@tak-ps/etl` for the build and push: its `bin/build.ts` hardcodes the destination ECR repository as `tak-vpc-<Environment>-cloudtak-tasks`, which does not match the `<stackname>-etltasks` repository used by TAK.NZ base-infra. The existing lookup of the repository through the `EcrEtlTasksRepoArn` CloudFormation export is kept unchanged
- :white_check_mark: Add a basic test suite (`npm test`, `node:test` run through `tsx`) covering the task's static config, input and output schemas and the manifest; the `lint` script now also covers `test/`. Previously `npm test` was `exit 0`
- :rocket: Use `Task.init()` for the local and Lambda entry points. No change in Lambda behaviour, `ETL_TOKEN` is always provided there
- :rocket: Require Node 24 (`engines` `>= 24`), and use Node 24 instead of 18 in the lint and deploy workflows, matching the Lambda base image and `@tak-ps/etl`
- :arrow_up: Update dependencies: `@tak-ps/etl` 10.22.2 (minimum raised to `^10.13.0`, which the manifest schema needs), `eslint` 10.12.0, `typescript-eslint` 8.71.1 and a new dev dependency `tsx` 4.23.15. `npm audit` now reports 0 vulnerabilities (8 before, 1 critical). `typescript` stays on 6.0.3 as `typescript-eslint` still limits supported versions to below 6.1.0
- :rocket: Add a `.dockerignore` so `.git`, `.github`, `node_modules`, `dist`, `test`, `docs`, `.agents`, `.env*` and markdown files are kept out of the image build context. `capabilities.json`, `task.ts`, `package*.json` and `tsconfig.json` stay in the context

- :arrow_up: Update GitHub Actions to releases that run on Node.js 24, clearing the Node.js 20 deprecation warnings: `actions/checkout` v7, `actions/setup-node` v7 and `aws-actions/configure-aws-credentials` v6. `aws-actions/amazon-ecr-login` v2 already runs on Node.js 24. Not yet run in CI on these versions
- :rocket: Pin the workflow runners to `ubuntu-24.04` instead of `ubuntu-latest`, so the `ubuntu-latest` migration to Ubuntu 26 (starting October 19, 2026) does not change the build environment unannounced
- :pencil2: Audit against CloudTAK issue #168 (manifest-based capabilities): no code or workflow changes were needed. The `feature:submit` permission matches the task's only CloudTAK API call (`this.submit()`), the manifest validates against `StaticCapabilitiesSchema`, and a local buildx build confirmed `capabilities.json` is in `/var/task` and lands as the `com.cloudtak.capabilities` annotation on the pushed manifest. The push to demo and reading the manifest from `GET /api/task/raw/...` still need to be checked after merge
### v1.0.0

- :tada: Initial Commit
