import gc
import pandas as pd

# Add all input files to a list
files = [
    'netflix_data/combined_data_1.txt',
    'netflix_data/combined_data_2.txt',
    'netflix_data/combined_data_3.txt',
    'netflix_data/combined_data_4.txt'
    ]

# Initialize lists to hold user, movies (items), and rating values
user_row = []
movie_col = []
rat_val = []

# Iterate through all files, organized by movie ID, and create a tuple of [userID, movieID, rating val] for each line
for file_name in files:
    print('processing {0}'.format(file_name))
    with open(file_name, "r") as f:
        movie = -1
        for line in f:
            if line.endswith(':\n'):
                movie = int(line[:-2])
                continue
            assert movie >= 0
            splitted = line.split(',')
            user = int(splitted[0])
            rating = int(splitted[1])
            user_row.append(user)
            movie_col.append(movie)
            rat_val.append(rating)
    gc.collect()

# Create a dataframe by concatenating the three lists produced
netflix_df = pd.DataFrame(list(zip(user_row, movie_col, rat_val)),
              columns=['user','movie', 'rating'])

# Output pre-processed data to csv/text file
netflix_df.to_csv(r'input\preprocessed_data.csv', header=False, index=False)
