from kaggle.api.kaggle_api_extended import KaggleApi


def download_data():
    """
    Authenticating and downloading Cornell University arxiv dataset from Kaggle
    Requires you to have a valid 'kaggle.json' token in your ~/.kaggle/ folder.

    Dependant libraries: kaggle
    """
    print("Starting kaggle data download")
    
    print("Authenticating to kaggle")
    api = KaggleApi()
    api.authenticate()

    dataset = 'Cornell-University/arxiv'
    print(f"Downloading {dataset}")
    api.dataset_download_files('Cornell-University/arxiv', path='../data/')
    
    print("Done")


if __name__ == "__main__":
    download_data()
