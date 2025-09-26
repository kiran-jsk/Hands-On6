# Music Streaming Analysis Using Spark Structured APIs

## Overview
Analyze music streaming behavior with PySpark by combining listening logs and track metadata to surface user preferences, genre loyalty, and late-night listening trends. The Spark job orchestrates every task and writes curated CSV outputs for downstream consumption.

## Dataset Description
- `listening_logs.csv`: Raw play events with `user_id`, `song_id`, timestamp, and play duration.
- `songs_metadata.csv`: Track attributes, including `title`, `artist`, `genre`, and `mood` for each `song_id` referenced in the logs.

## Repository Structure
- `main.py`: Entry point that runs all Spark Structured API analyses.
- `datagen.py`: Utility to regenerate the sample CSV inputs.
- `listening_logs.csv`, `songs_metadata.csv`: Ready-to-use sample datasets.
- `output/`: Destination directory populated with per-task results.

## Output Directory Structure
- `output/user_favorite_genres/`: CSV export of each user’s top genre and supporting play count.
- `output/avg_listen_time_per_song/`: Per-song average listening duration with titles and artists.
- `output/genre_loyalty_scores/`: Users whose favorite genre accounts for at most 75% of their plays.
- `output/night_owl_users/`: Late-night activity summary for users active between 00:00 and 04:59.

## Tasks and Outputs
- **Task 1 – Favorite Genre Identification**: Joins logs with metadata, counts plays by `(user_id, genre)`, and selects the top genre per user using a window function. Results stored in `output/user_favorite_genres/`.
- **Task 2 – Average Listen Time Per Song**: Aggregates `duration_sec` per `song_id` and enriches with song details. Outputs written to `output/avg_listen_time_per_song/`.
- **Task 3 – Genre Loyalty Scores**: Calculates each user’s favorite-genre share of total plays and filters for loyalty scores less than or equal to `0.75`, saving the results to `output/genre_loyalty_scores/`.
- **Task 4 – Night Owl Detection**: Derives the play hour from timestamps and summarizes users with activity between midnight and 5 AM in `output/night_owl_users/`.

## Execution Instructions
### Prerequisites
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

**Python 3.x**
- Download and install Python from the official website.
- Verify installation:
  ```bash
  python3 --version
  ```

**PySpark**
- Install using pip:
  ```bash
  pip install pyspark
  ```

**Apache Spark**
- Download and install from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
- Verify installation by running:
  ```bash
  spark-submit --version
  ```

### Running the Analysis Tasks
#### Running Locally
- **Generate the input** (optional if sample CSVs suffice):
  ```bash
  python3 datagen.py
  ```
- **Execute each task using `spark-submit`** (runs all tasks in sequence):
  ```bash
  spark-submit main.py
  ```
- **Verify the outputs** by inspecting the `output/` directory:
  ```bash
  ls output/
  ```

### Errors and Resolutions
- If `spark-submit` cannot bind to a local port, rerun the job outside restrictive sandboxes or provide the necessary permissions.
- When encountering `ModuleNotFoundError: No module named 'pyspark'`, install PySpark for the Python interpreter you use to launch the job (e.g., `pip3 install pyspark`).
