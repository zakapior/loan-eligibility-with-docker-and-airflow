"""
A short script, that will download the Loan Eligible Dataset from Kaggle using
the kaggle Python package. It is intended to be run from within Docker Python
container. Kaggle authentication variables should be provided in the
environmet as operating system variables.
"""

from kaggle.api.kaggle_api_extended import KaggleApi

DATASET = "vikasukani/loan-eligible-dataset"
OUTPUT_PATH = "/usr/src/app/output"


def download_kaggle_dataset_files(
    dataset_repository: str, output_path: str
) -> None:
    """
    Downloads the DATASET dataset files into OUTPUT_PATH using Kaggle Python package.

    Arguments:
        None

    Returns:
        filepaths: list[str] - list of downloaded files will full path
    """
    api = KaggleApi()
    api.authenticate()

    try:
        datafiles = api.dataset_list_files(dataset=dataset_repository).files
        api.dataset_download_files(
            dataset_repository, unzip=True, path=output_path
        )
    except Exception as e:
        print("Fetching data from Kaggle resulted with an error:", e)

    filepaths = [f"{output_path}/{datafile}" for datafile in datafiles]

    return filepaths


if __name__ == "__main__":
    datafiles = download_kaggle_dataset_files(DATASET, OUTPUT_PATH)
    print(" ".join(datafiles))
