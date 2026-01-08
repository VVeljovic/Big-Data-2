import pandas as pd

input_file = '../spotify_dataset.csv'
output_file = 'cleaned_spotify_dataset.csv'

def clean_data(input_file, output_file):
    df = pd.read_csv(input_file)

    df.drop(columns=['text'], inplace=True)
    df.to_csv(output_file, index=False)

clean_data(input_file, output_file)