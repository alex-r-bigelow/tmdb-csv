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
parser.add_argument('-t', '--top_rated', {
  help: 'number of top_rated movie pages to fetch',
});
parser.add_argument('-p', '--popularity', {
  help: 'number of top-popularity movie pages to fetch',
});
parser.add_argument('-v', '--vote_count', {
  help: 'number of most-voted movie pages to fetch',
});
parser.add_argument('-r', '--revenue', {
  help: 'number of top revenue movie pages to fetch',
});

const args = parser.parse_args();

const FIXED_PROMOTABLE_KEYS = ['id', 'iso_3166_1', 'iso_639_1'];

let currentFetch = Promise.resolve();
const rateLimitedFetch = async (url) => {
  currentFetch = currentFetch.then(
    () =>
      new Promise((resolve) => setTimeout(() => fetch(url).then(resolve), 0))
  );
  return currentFetch;
};

const getMovieByTopRated = async (pageNo) =>
  rateLimitedFetch(
    `https://api.themoviedb.org/3/movie/top_rated?api_key=${args.key}&language=en-US&page=${pageNo}`
  ).then((result) => result.json());

const getMoviesByPopularity = async (pageNo) =>
  rateLimitedFetch(
    `https://api.themoviedb.org/3/discover/movie?sort_by=popularity.desc&api_key=${args.key}&language=en-US&page=${pageNo}`
  ).then((result) => result.json());

const getMoviesByMostVotes = async (pageNo) =>
  rateLimitedFetch(
    `https://api.themoviedb.org/3/discover/movie?sort_by=vote_count.desc&api_key=${args.key}&language=en-US&page=${pageNo}`
  ).then((result) => result.json());

const getMoviesByTopRevenue = async (pageNo) =>
  rateLimitedFetch(
    `https://api.themoviedb.org/3/discover/movie?sort_by=revenue.desc&api_key=${args.key}&language=en-US&page=${pageNo}`
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

const getCompanyDetails = async (companyId) =>
  rateLimitedFetch(
    `https://api.themoviedb.org/3/company/${companyId}?api_key=${args.key}&language=en-US`
  ).then((result) => result.json());

const run = async () => {
  const moviesPath = path.join(args.csv, 'movies.csv');
  const castPath = path.join(args.csv, 'cast.csv');
  const crewPath = path.join(args.csv, 'crew.csv');
  const peoplePath = path.join(args.csv, 'people.csv');
  const companyPath = path.join(args.csv, 'company.csv');

  const baseFiles = {
    movies: {
      objKey: 'id',
      includedKeys: new Set(),
      skipKeys: new Set(['adult', 'backdrop_path', 'poster_path']),
      knownObjectKeys: new Set([
        'belongs_to_collection',
        'genres',
        'production_companies',
        'production_countries',
        'spoken_languages',
      ]),
      headers: [],
      path: moviesPath,
      file: fs.openSync(moviesPath, 'w+'),
    },
    cast: {
      objKey: 'credit_id',
      includedKeys: new Set(),
      skipKeys: new Set(['adult', 'profile_path']),
      headers: [],
      path: castPath,
      file: fs.openSync(castPath, 'w+'),
    },
    crew: {
      objKey: 'credit_id',
      includedKeys: new Set(),
      skipKeys: new Set(['adult', 'profile_path']),
      headers: [],
      path: crewPath,
      file: fs.openSync(crewPath, 'w+'),
    },
    people: {
      objKey: 'id',
      includedKeys: new Set(),
      skipKeys: new Set(['adult', 'biography', 'profile_path']),
      headers: [],
      path: peoplePath,
      file: fs.openSync(peoplePath, 'w+'),
    },
    company: {
      objKey: 'id',
      includedKeys: new Set(),
      skipKeys: new Set(['logo_path']),
      knownObjectKeys: new Set(['parent_company']),
      headers: [],
      path: companyPath,
      file: fs.openSync(companyPath, 'w+'),
    },
  };
  const junctionFiles = {};

  const checkHeaders = (data, baseFile) => {
    if (baseFile.headers.length === 0) {
      const sourceKey = path.basename(baseFile.path, '.csv');
      baseFile.headers = Object.keys(data).filter((key) => {
        if (baseFile.skipKeys?.has(key)) {
          return false;
        }
        if (
          baseFile.knownObjectKeys?.has(key) ||
          (typeof data[key] === 'object' &&
            data[key] !== null &&
            Object.keys(data[key]).length > 0)
        ) {
          const promotedKey = key;
          const junctionKey = `${sourceKey}_${promotedKey}`;
          const isArray = data[promotedKey] instanceof Array;
          const nestedSample = isArray
            ? data[promotedKey][0]
            : data[promotedKey];
          if (typeof nestedSample === 'string') {
            return true;
          }
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
            if (!baseFile.knownObjectKeys?.has(promotedKey)) {
              console.log(
                `Promoting nested objects as ${promotedFilePath} that look like: ${JSON.stringify(
                  nestedSample,
                  null,
                  2
                )}`
              );
            }
            baseFiles[promotedKey] = {
              objKey: promotedObjKey,
              includedKeys: new Set(),
              headers: [],
              path: promotedFilePath,
              file: fs.openSync(promotedFilePath, 'w+'),
            };
            const junctionFilePath = path.join(args.csv, `${junctionKey}.csv`);
            junctionFiles[junctionKey] = {
              headers: [],
              path: junctionFilePath,
              file: fs.openSync(junctionFilePath, 'w+'),
            };
          }
          checkHeaders(nestedSample, baseFiles[promotedKey]);
          checkHeaders(
            { [sourceKey]: '', [promotedKey]: '' },
            junctionFiles[junctionKey]
          );
          return false;
        }
        return true;
      });
      const headerString =
        Papa.unparse([baseFile.headers], {
          columns: baseFile.headers,
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
        columns: baseFile.headers,
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

  const queryCompany = async (company) => {
    if (!baseFiles.company.includedKeys.has(company.id)) {
      console.log(`Querying company: ${company.name}`);
      const companyDetails = await getCompanyDetails(company.id);
      checkHeaders(companyDetails, baseFiles.company);
      writeLine(companyDetails, baseFiles.company);

      if (companyDetails.parent_company) {
        const parentCompanies =
          companyDetails.parent_company instanceof Array
            ? companyDetails.parent_company
            : [companyDetails.parent_company];
        for await (const parentCompany of parentCompanies) {
          await queryCompany(parentCompany);
        }
      }
    }
  };

  const queryMovie = async (movie) => {
    if (baseFiles.movies.includedKeys.has(movie.id)) {
      console.log(`Already Queried movie: ${movie.title} (skipping)`);
    } else {
      console.log(`Querying movie: ${movie.title}`);
      const movieDetails = await getMovieDetails(movie.id);
      checkHeaders(movieDetails, baseFiles.movies);
      writeLine(movieDetails, baseFiles.movies);

      if (movieDetails.production_companies) {
        const companies =
          movieDetails.production_companies instanceof Array
            ? movieDetails.production_companies
            : [movieDetails.production_companies];
        for await (const company of companies) {
          await queryCompany(company);
        }
      }

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
  };

  const queryMoviePages = async (pageCount, queryFunc) => {
    for (let pageNo = 1; pageNo <= pageCount; pageNo++) {
      const movies = await queryFunc(pageNo);
      console.log(
        `Processing ${movies.results.length}${
          pageNo > 1 ? ' more' : ''
        } movies...`
      );

      for await (const movie of movies.results) {
        await queryMovie(movie);
      }
    }
  };

  if (args.top_rated) {
    await queryMoviePages(parseInt(args.top_rated), getMovieByTopRated);
  }
  if (args.popularity) {
    await queryMoviePages(parseInt(args.popularity), getMoviesByPopularity);
  }
  if (args.vote_count) {
    await queryMoviePages(parseInt(args.vote_count), getMoviesByMostVotes);
  }
  if (args.revenue) {
    await queryMoviePages(parseInt(args.revenue), getMoviesByTopRevenue);
  }

  Object.values(baseFiles).forEach(({ file }) => fs.close(file));
  Object.values(junctionFiles).forEach(({ file }) => fs.close(file));
};
run();
