## Setting up your environment

### Installing the dependencies

```sh
poetry install
```

### Activate the virtualenv

```sh
poetry shell
```

### Configure the environment variables

Make a copy of `env.sample` (provided in the root of the project) called `.env`

```sh
cp env.sample .env
```

Then provide the values as follows:

- `DETA_SDK_TEST_PROJECT_KEY` – Test project key (create a new Deta project for testing and grab the generated key).
- `DETA_SDK_TEST_BASE_NAME` – Name of your Base, default is fine.
- `DETA_SDK_TEST_DRIVE_NAME` – Name of your Drive, default is fine.
- `DETA_SDK_TEST_DRIVE_HOST` – Host URL, default is fine.

### Run the tests

```sh
poetry run pytest
```

🎉 Now you are ready to contribute!

### How to contribute

1. Git clone and make a feature branch
1. Make a draft PR
1. Make your changes to the feature branch
1. Mark draft as ready for review
