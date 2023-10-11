# tmdb-csv

A simple script for downloading movies as CSVs from TMDB

### Setup

- Install `node.js`; the best way to do this is usually via [nvm](https://github.com/nvm-sh/nvm)

```
git clone git@github.com:alex-r-bigelow/tmdb-csv.git
cd tmdb-csv
npm install

./getTmdbData.mjs --help
```

You will need to sign up for a TMDB API key, that you should supply as the `--key` argument.

## About the example `output`

This script and example data were originally created and collected as part of a [free academic community training workshop](https://researchbazaar.arizona.edu/resbaz/Arizona2023/) learning about SPARQL, etc.

The contents of the current `output` folder were generated with a balance of `-t`, `-p`, `-v`, and `-r` settings on July 27, 2023 for educational purposes. Especially if you are using this repository outside of an educational context, please first check the [TMDB API terms of use](https://www.themoviedb.org/documentation/api/terms-of-use?language=en-US)—it might be best to consider applying for your own TMDB API key and running the script yourself, rather than using this example data directly (which will probably be fairly out-of-date by the time you're looking at it anyway).

## About `bechdeltest.csv`

`bechdeltest.csv` is a direct JSON -> CSV conversion from the [Bechdel Test API](https://bechdeltest.com/api/v1/doc)'s `getAllMovies` dump, and is also included in this repo as part of the community workshop and falls under the [CC BY-NC 3.0 license](https://creativecommons.org/licenses/by-nc/3.0/)
