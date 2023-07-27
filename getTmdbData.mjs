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

const FIXED_PROMOTABLE_KEYS = ['id', 'iso_3166_1', 'iso_639_1'];

let currentFetch = Promise.resolve();
const rateLimitedFetch = async (url) => {
  currentFetch = currentFetch.then(
    () =>
      new Promise((resolve) => setTimeout(() => fetch(url).then(resolve), 100))
  );
  return currentFetch;
};

const getMovieByTopRatedPage = async (pageNo) =>
  rateLimitedFetch(
    `https://api.themoviedb.org/3/movie/top_rated?api_key=${args.key}&language=en-US&page=${pageNo}`
  ).then((result) => result.json());

const getMovieDetails = async (movieId) =>
  rateLimitedFetch(
    `https://api.themoviedb.org/3/movie/${movieId}?api_key=${args.key}&language=en-US`
  ).then((result) => result.json());

const getMovieCredits = async (movieId) =>
  rateLimitedFetch(
    `https://api.themoviedb.org/3/movie/${movieId}/credits?api_key=${args.key}&language=en-US`
  ).then((result) => result.json());

const getPersonDetails = async (personId) =>
  rateLimitedFetch(
    `https://api.themoviedb.org/3/person/${personId}?api_key=${args.key}&language=en-US`
  ).then((result) => result.json());

const run = async () => {
  const moviesPath = path.join(args.csv, 'movies.csv');
  const castPath = path.join(args.csv, 'cast.csv');
  const crewPath = path.join(args.csv, 'crew.csv');
  const peoplePath = path.join(args.csv, 'people.csv');

  const baseFiles = {
    movies: {
      objKey: 'id',
      includedKeys: new Set(),
      headers: { all: new Set(), order: [] },
      path: moviesPath,
      file: fs.openSync(moviesPath, 'w+'),
    },
    cast: {
      objKey: 'credit_id',
      includedKeys: new Set(),
      headers: { all: new Set(), order: [] },
      path: castPath,
      file: fs.openSync(castPath, 'w+'),
    },
    crew: {
      objKey: 'credit_id',
      includedKeys: new Set(),
      headers: { all: new Set(), order: [] },
      path: crewPath,
      file: fs.openSync(crewPath, 'w+'),
    },
    people: {
      objKey: 'id',
      includedKeys: new Set(),
      headers: { all: new Set(), order: [] },
      path: peoplePath,
      file: fs.openSync(peoplePath, 'w+'),
    },
  };
  const junctionFiles = {};

  const checkHeaders = (data, baseFile) => {
    if (baseFile.headers.all.size === 0) {
      const sourceKey = path.basename(baseFile.path, '.csv');
      baseFile.headers.order = Object.keys(data).filter((key) => {
        const isObject = typeof data[key] === 'object' && data[key] !== null;
        if (isObject && Object.keys(data[key]).length > 0) {
          const promotedKey = key;
          const junctionKey = `${sourceKey}_${promotedKey}`;
          const isArray = data[promotedKey] instanceof Array;
          const nestedSample = isArray
            ? data[promotedKey][0]
            : data[promotedKey];
          const promotedObjKey = FIXED_PROMOTABLE_KEYS.find(
            (pKey) => nestedSample[pKey] !== undefined
          );
          if (promotedObjKey === undefined) {
            console.log(
              `Skipping nested object (couldn't find a known key for promotion): ${JSON.stringify(
                nestedSample,
                null,
                2
              )}`
            );
            return false;
          }
          if (!baseFiles[promotedKey]) {
            const promotedFilePath = path.join(args.csv, `${promotedKey}.csv`);
            console.log(
              `Promoting nested objects as ${promotedFilePath} that look like: ${JSON.stringify(
                nestedSample,
                null,
                2
              )}`
            );
            baseFiles[promotedKey] = {
              objKey: promotedObjKey,
              includedKeys: new Set(),
              headers: { all: new Set(), order: [] },
              path: promotedFilePath,
              file: fs.openSync(promotedFilePath, 'w+'),
            };
            const junctionFilePath = path.join(args.csv, `${junctionKey}.csv`);
            junctionFiles[junctionKey] = {
              headers: { all: new Set(), order: [] },
              path: junctionFilePath,
              file: fs.openSync(junctionFilePath, 'w+'),
            };
          }
          checkHeaders(nestedSample, baseFiles[promotedKey]);
          checkHeaders(
            { [sourceKey]: '', [promotedKey]: '' },
            junctionFiles[junctionKey]
          );
        }
        return !isObject;
      });
      baseFile.headers.all = new Set(baseFile.headers.order);
      const headerString =
        Papa.unparse([baseFile.headers.order], {
          columns: baseFile.headers.order,
          header: true,
        }) + '\n';
      fs.writeSync(baseFile.file, headerString);
    } else {
      // TODO: check and warn if we find additional headers we weren't expecting
    }
  };

  const writeLine = (data, baseFile) => {
    if (baseFile.includedKeys) {
      if (baseFile.includedKeys.has(data[baseFile.objKey])) {
        return;
      }
      baseFile.includedKeys.add(data[baseFile.objKey]);
    }
    const outputString =
      Papa.unparse([data], {
        columns: baseFile.headers.order,
        header: false,
        newline: '\n',
      }) + '\n';
    fs.writeSync(baseFile.file, outputString);
    const sourceKey = path.basename(baseFile.path, '.csv');
    Object.keys(baseFiles).forEach((promotedKey) => {
      if (
        promotedKey in data &&
        typeof data[promotedKey] === 'object' &&
        data[promotedKey] !== null
      ) {
        const lines =
          data[promotedKey] instanceof Array
            ? data[promotedKey]
            : [data[promotedKey]];
        lines.forEach((line) => {
          writeLine(line, baseFiles[promotedKey]);
          const junctionKey = `${sourceKey}_${promotedKey}`;
          writeLine(
            {
              [sourceKey]: data[baseFile.objKey],
              [promotedKey]: line[baseFiles[promotedKey].objKey],
            },
            junctionFiles[junctionKey]
          );
        });
      }
    });
  };

  const queryPerson = async (credit) => {
    if (!baseFiles.people.includedKeys.has(credit.id)) {
      console.log(`Querying person: ${credit.name}`);
      const person = await getPersonDetails(credit.id);
      checkHeaders(person, baseFiles.people);
      writeLine(person, baseFiles.people);
    }
  };

  for (let pageNo = 1; pageNo <= parseInt(args.pages); pageNo++) {
    const movies = await getMovieByTopRatedPage(pageNo);
    console.log(
      `Processing ${movies.results.length}${
        pageNo > 1 ? ' more' : ''
      } movies...`
    );

    for await (const movie of movies.results) {
      console.log(`Querying movie: ${movie.title}`);
      const movieDetails = await getMovieDetails(movie.id);
      checkHeaders(movieDetails, baseFiles.movies);
      writeLine(movieDetails, baseFiles.movies);

      console.log(`Querying credits for movie: ${movie.title}`);
      const { cast, crew } = await getMovieCredits(movie.id);

      for await (const rawCredit of cast) {
        const credit = { ...rawCredit, movieId: movie.id };
        checkHeaders(credit, baseFiles.cast);
        writeLine(credit, baseFiles.cast);
        await queryPerson(credit);
      }
      for await (const rawCredit of crew) {
        const credit = { ...rawCredit, movieId: movie.id };
        checkHeaders(credit, baseFiles.crew);
        writeLine(credit, baseFiles.crew);
        await queryPerson(credit);
      }
    }
  }

  Object.values(baseFiles).forEach(({ file }) => fs.close(file));
  Object.values(junctionFiles).forEach(({ file }) => fs.close(file));
};
run();
