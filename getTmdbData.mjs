#! /usr/bin/env node

import { ArgumentParser } from 'argparse';
import Papa from 'papaparse';
import * as fs from 'fs';
import path from 'path';
import fetch from 'node-fetch';

const parser = new ArgumentParser({
  description: 'Pulls data from TMDB',
});

parser.add_argument('-k', '--key', {
  help: 'API key',
  required: true,
});
parser.add_argument('-c', '--csv', {
  help: 'output relational CSV directory',
  required: true,
});
parser.add_argument('-p', '--pages', {
  help: 'number of top-rated movie pages to fetch',
  default: 1,
});

const args = parser.parse_args();

const getMovieByTopRatedPage = async (pageNo) =>
  fetch(
    `https://api.themoviedb.org/3/movie/top_rated?api_key=${args.key}&language=en-US&page=${pageNo}`
  ).then((result) => result.json());

const getMovieDetails = async (movieId) =>
  fetch(
    `https://api.themoviedb.org/3/movie/${movieId}?api_key=${args.key}&language=en-US`
  ).then((result) => result.json());

const getMovieCredits = async (movieId) =>
  fetch(
    `https://api.themoviedb.org/3/movie/${movieId}/credits?api_key=${args.key}&language=en-US`
  ).then((result) => result.json());

const getPersonDetails = async (personId) =>
  fetch(
    `https://api.themoviedb.org/3/person/${personId}?api_key=${args.key}&language=en-US`
  ).then((result) => result.json());

const run = async () => {
  const movieHeaders = { all: new Set(), order: [] };
  const moviesFile = fs.openSync(path.join(args.csv, 'movies.csv'), 'w+');

  const castHeaders = { all: new Set(), order: [] };
  const castFile = fs.openSync(path.join(args.csv, 'cast.csv'), 'w+');
  const crewHeaders = { all: new Set(), order: [] };
  const crewFile = fs.openSync(path.join(args.csv, 'crew.csv'), 'w+');

  const peopleHeaders = { all: new Set(), order: [] };
  const peopleFile = fs.openSync(path.join(args.csv, 'people.csv'), 'w+');
  const queriedPersonIds = new Set();

  const checkHeaders = (data, headers, outputFile) => {
    if (headers.all.size === 0) {
      headers.order = Object.keys(data).filter(
        (key) => typeof data[key] !== 'object'
      );
      headers.all = new Set(headers.order);
      const headerString =
        Papa.unparse([headers.order], {
          columns: headers.order,
          header: true,
        }) + '\n';
      fs.writeSync(outputFile, headerString);
    }
  };

  const writeLine = (data, headers, outputFile) => {
    const outputString =
      Papa.unparse([data], {
        columns: headers.order,
        header: false,
        newline: '\n',
      }) + '\n';
    fs.writeSync(outputFile, outputString);
  };

  const queryPerson = async (credit) => {
    if (!queriedPersonIds.has(credit.id)) {
      console.log(`Loading details for person: ${credit.name}`);
      const person = await getPersonDetails(credit.id);
      checkHeaders(person, peopleHeaders, peopleFile);
      writeLine(person, peopleHeaders, peopleFile);
    }
  };

  for (let pageNo = 1; pageNo < parseInt(args.pages); pageNo++) {
    const movies = await getMovieByTopRatedPage(pageNo);

    for await (const movie of movies.results) {
      console.log(`Loading details for movie: ${movie.title}`);
      const movieDetails = await getMovieDetails(movie.id);
      checkHeaders(movieDetails, movieHeaders, moviesFile);
      writeLine(movieDetails, movieHeaders, moviesFile);

      console.log(`Loading credits for movie: ${movie.title}`);
      const { cast, crew } = await getMovieCredits(movie.id);

      for await (const rawCredit of cast) {
        const credit = { ...rawCredit, movieId: movie.id };
        checkHeaders(credit, castHeaders, castFile);
        writeLine(credit, castHeaders, castFile);
        queryPerson(credit);
      }
      for await (const rawCredit of crew) {
        const credit = { ...rawCredit, movieId: movie.id };
        checkHeaders(credit, crewHeaders, crewFile);
        writeLine(credit, crewHeaders, crewFile);
        queryPerson(credit);
      }
    }
  }

  fs.close(moviesFile);
  fs.close(castFile);
  fs.close(crewFile);
  fs.close(peopleFile);
};
run();
