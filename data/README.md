# Datasets


## ArXiv Metadata

This dataset contains metadata on 1.5MM+ scholarly in a variety of fields, from: https://www.kaggle.com/Cornell-University/arxiv

In order to download this via the script, you'll need a Kaggle user and have downloaded your kaggle credentials and token to your ~/.kaggle/ folder on your local computer.

A download script for downloading this can be found [here](./../setup/datasets/kaggle.py).

If you wish to download manually, login to [kaggle.com](https://www.kaggle.com) and download the 'arxiv.zip' file to the /data/ folder in this project.


## ArXiv Subject Classifications

List of categories scraped from: https://arxiv.org/help/api/user-manual

A download script for downloading this can be found [here](./../setup/datasets/classifications.py).


## **To download**

To download dataset, execute from setup folder as:

```python
>>> python ./download_datasets.py
```

