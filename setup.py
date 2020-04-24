from setuptools import setup
import os
from pathlib import Path

detected_version = None
version_filepath = "VERSION"

if "VERSION" in os.environ:
    detected_version = os.environ["VERSION"]
    if "/" in detected_version:
        detected_version = detected_version.split("/")[-1]
if not detected_version and os.path.exists(version_filepath):
    detected_version = Path(version_filepath).read_text()
    if len(detected_version.split(".")) <= 3:
        if "BUILD_NUMBER" in os.environ:
            detected_version = f"{detected_version}.{os.environ['BUILD_NUMBER']}"
if not detected_version:
    raise RuntimeError("Error. Could not detect version.")
detected_version = detected_version.replace(".dev0", "")
if os.environ.get("BRANCH_NAME", "unknown") not in ["master", "refs/heads/master"]:
    detected_version = f"{detected_version}.dev0"

detected_version = detected_version.lstrip("v")
print(f"Detected version: {detected_version}")
Path(version_filepath).write_text(f"v{detected_version}")

setup(
    name="slalom.dataops",
    packages=["slalom.dataops"],
    version=detected_version,
    license="MIT",
    description="Slalom GGP libary for DataOps automation",
    author="AJ Steers",
    author_email="aj.steers@slalom.com",
    url="https://bitbucket.org/slalom-consulting/dataops-tools/",
    download_url="https://github.com/slalom-ggp/dataops-tools/archive/v_0.1.tar.gz",
    keywords=["DATAOPS", "SLALOM", "DATA", "AUTOMATION", "CI/CD", "DEVOPS"],
    package_data={"": [version_filepath]},
    entry_points={
        "console_scripts": [
            # Register CLI commands:
            "s-docker = slalom.dataops.dockerutils:main",
            "s-infra = slalom.dataops.infra:main",
            "s-io = slalom.dataops.io:main",
            "s-spark = slalom.dataops.sparkutils:main",
            "s-tap = slalom.dataops.taputils:main",
        ]
    },
    include_package_data=True,
    install_requires=[
        "docker",
        "fire",
        "joblib",
        "junit-xml",
        "psutil",
        "pyyaml",
        "tqdm",
        "xmlrunner",
    ],
    extras_require={
        "AWS": ["awscli", "boto3", "s3fs"],
        "Azure": ["azure"],
        "Pandas": ["pandas"],
        "S3": ["boto3", "s3fs"],
        "Spark": ["pyspark"],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",  # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
    ],
)
Path(version_filepath).write_text(f"v{detected_version.replace('.dev0', '')}")
