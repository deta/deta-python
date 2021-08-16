# Deta Python Library (SDK)

Supports Python 3.5+ only. [Read the docs.](https://docs.deta.sh/docs/base/sdk)  

Install from PyPi

```sh
pip install deta
```

## How to contribute
1. Make a feature branch
2. Make a draft PR
3. Make your changes to the feature branch
4. Mark draft as ready for review

## How to release
1. Add changes to `CHANGELOG.md`
2. Merge the `master` branch with the `release` branch.
3. After scripts finish, update release and tag with relevant info

## How to run tests
1. Create an `.env` file with the following variables
    - `DETA_SDK_TEST_PROJECT_KEY`: Project Key for your deta project
    - `DETA_SDK_TEST_DRIVE_NAME` : Name of your drive 
    - `DETA_SDK_TEST_DRIVE_HOST` : Host URL (default `"drive.deta.sh"`)
2. Run
   ```
   python tests.py
   ``` 