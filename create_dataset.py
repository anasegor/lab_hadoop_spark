from datasets import load_dataset

ds = load_dataset("maharshipandya/spotify-tracks-dataset")
ds["train"].to_csv("spotify-tracks-dataset.csv")
