#!/usr/bin/env python3

import gzip
import os
import threading
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
from queue import Queue

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from dotenv import load_dotenv
from lxml import html
from orjson import loads

# Import from your rpscrape project
from utils.network import NetworkClient
from utils.paths import Paths, build_paths, RequestKey
from utils.settings import Settings
from utils.race import Race, VoidRaceError

_ = load_dotenv()

app = Flask(__name__)
CORS(app)

settings = Settings()

RACE_TYPES: dict[str, set[str]] = {
    'flat': {'Flat'},
    'jumps': {'Chase', 'Hurdle', 'NH Flat'},
}

# Global state for tracking scraping jobs
scraping_jobs: Dict[str, Dict[str, Any]] = {}
job_queue = Queue()


class ScrapingJob:
    """Represents a single scraping job."""
    
    def __init__(self, job_id: str, config: Dict[str, Any]):
        self.job_id = job_id
        self.config = config
        self.status = "queued"
        self.progress = 0.0
        self.message = "Waiting to start..."
        self.log = []
        self.error = None
        self.start_time = None
        self.end_time = None
        self.output_file = None
        self.cancel_requested = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            'job_id': self.job_id,
            'status': self.status,
            'progress': self.progress,
            'message': self.message,
            'log': self.log[-100:],  # Last 100 log entries
            'error': self.error,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'output_file': self.output_file,
        }

    def log_message(self, message: str, level: str = 'info'):
        """Add a log message."""
        timestamp = datetime.now().isoformat()
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        self.log.append(log_entry)
        print(f"[{self.job_id}] {level.upper()}: {message}")

    def update_progress(self, progress: float, message: str, level: str = 'info'):
        """Update progress and message."""
        self.progress = min(progress, 100.0)
        self.message = message
        self.log_message(message, level)


def get_date_range_for_year(year: str) -> list[date]:
    """Get all dates in a year."""
    year_int = int(year)
    start_date = date(year_int, 1, 1)
    end_date = date(year_int, 12, 31)
    
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    
    return dates


def filter_races_by_type(urls: set[str], race_types: list[str]) -> set[str]:
    """Filter race URLs based on race types to scrape."""
    if 'both' in race_types or (len(race_types) == 2):
        # Keep all races
        return urls
    return urls


def get_all_races_by_years(
    years: list[str], client: NetworkClient, job: ScrapingJob
) -> list[str]:
    """Get all races for specific years (all courses, all dates)."""
    urls: set[str] = set()
    
    # Generate date ranges for all years
    all_dates = []
    for year in years:
        try:
            all_dates.extend(get_date_range_for_year(year))
        except ValueError as e:
            job.log_message(f'Invalid year {year}: {str(e)}', 'error')
            continue
    
    total_dates = len(all_dates)
    job.log_message(f'Generated {total_dates} dates to scrape from {len(years)} year(s)')
    
    for date_idx, race_date in enumerate(all_dates):
        if job.cancel_requested:
            raise Exception("Scraping cancelled by user")
        
        # Update progress
        date_progress = (date_idx / total_dates) * 50  # First 50% is URL fetching
        job.update_progress(10 + date_progress, f'Fetching URLs for {race_date} ({date_idx + 1}/{total_dates})...')

        url = f'https://www.racingpost.com/results/{race_date}'

        try:
            _, response = client.get(url)
            doc = html.fromstring(response.content)

            # Get all race links regardless of course
            races = doc.xpath('//a[@data-test-selector="link-listCourseNameLink"]')
            
            for race in races:
                try:
                    race_url = f'https://www.racingpost.com{race.attrib["href"]}'
                    urls.add(race_url)
                except (KeyError, AttributeError):
                    continue

            if len(urls) % 100 == 0:
                job.log_message(f'Found {len(urls)} races so far ({date_idx + 1}/{total_dates} dates processed)')

        except Exception as e:
            job.log_message(f'Error fetching {url}: {str(e)}', 'warning')
            continue

    return sorted(urls, key=lambda url: (url.split('/')[6], url.split('/')[5]))


def get_all_races_by_date(
    dates: list[date], client: NetworkClient, job: ScrapingJob
) -> list[str]:
    """Get all races for specific dates (all courses)."""
    urls: set[str] = set()
    total_dates = len(dates)

    for date_idx, race_date in enumerate(dates):
        if job.cancel_requested:
            raise Exception("Scraping cancelled by user")

        url = f'https://www.racingpost.com/results/{race_date}'
        
        # Update progress
        date_progress = (date_idx / total_dates) * 50  # First 50% is URL fetching
        job.update_progress(10 + date_progress, f'Fetching races from {race_date} ({date_idx + 1}/{total_dates})...')

        try:
            _, response = client.get(url)
            doc = html.fromstring(response.content)

            # Get all race links regardless of course
            races = doc.xpath('//a[@data-test-selector="link-listCourseNameLink"]')
            
            for race in races:
                try:
                    race_url = f'https://www.racingpost.com{race.attrib["href"]}'
                    urls.add(race_url)
                except (KeyError, AttributeError):
                    continue

            job.log_message(f'Found {len(urls)} races so far from {race_date}')

        except Exception as e:
            job.log_message(f'Error fetching {url}: {str(e)}', 'error')
            continue

    return sorted(urls, key=lambda url: (url.split('/')[6], url.split('/')[5]))


def get_race_urls(
    years: list[str], tracks: list[str], race_types: list[str], client: NetworkClient, job: ScrapingJob
) -> list[str]:
    """Get race URLs by years for specific tracks."""
    url_course_base = 'https://www.racingpost.com:443/profile/course/filter/results'
    url_result_base = 'https://www.racingpost.com/results'

    urls: set[str] = set()
    total_iterations = len(years) * len(tracks) * len(race_types)
    current = 0

    for course in tracks:
        for year in years:
            for race_type in race_types:
                if job.cancel_requested:
                    raise Exception("Scraping cancelled by user")

                current += 1
                job.update_progress(5 + (current / total_iterations) * 5, f'Fetching {course} ({year}) - {race_type}...')

                # Use course name as course_id (simplified)
                race_list_url = f'{url_course_base}/{course}/{year}/{race_type}/all-races'

                try:
                    job.log_message(f'Fetching {race_type} races from {course} ({year})...')
                    status, response = client.get(race_list_url)

                    if status != 200:
                        job.log_message(f'Failed to get race urls. Status: {status}', 'warning')
                        continue

                    data = loads(response.text).get('data', {})
                    races = data.get('principleRaceResults', [])

                    if not races:
                        job.log_message(f'No {race_type} races found for {course} in {year}')
                        continue

                    for race in races:
                        race_date = race['raceDatetime'][:10]
                        race_id = race['raceInstanceUid']
                        race_url = f'{url_result_base}/{course}/{course}/{race_date}/{race_id}'
                        urls.add(race_url.replace(' ', '-').replace("'", ''))

                    job.log_message(f'Found {len(urls)} races so far from {course} ({year}) - {race_type}')

                except Exception as e:
                    job.log_message(f'Error fetching {race_list_url}: {str(e)}', 'error')
                    continue

    return sorted(urls, key=lambda url: (url.split('/')[6], url.split('/')[5]))


def get_race_urls_date(
    dates: list[date], tracks: list[str], client: NetworkClient, job: ScrapingJob
) -> list[str]:
    """Get race URLs by dates for specific courses."""
    urls: set[str] = set()
    track_set: set[str] = set(tracks)
    total_dates = len(dates)

    for date_idx, race_date in enumerate(dates):
        if job.cancel_requested:
            raise Exception("Scraping cancelled by user")

        job.update_progress(5 + (date_idx / total_dates) * 5, f'Fetching races from {race_date}...')
        url = f'https://www.racingpost.com/results/{race_date}'

        try:
            job.log_message(f'Fetching races from {race_date}...')
            _, response = client.get(url)
            doc = html.fromstring(response.content)

            races = doc.xpath('//a[@data-test-selector="link-listCourseNameLink"]')
            for race in races:
                try:
                    course_id = race.attrib['href'].split('/')[2]
                    if course_id in track_set:
                        urls.add(f'https://www.racingpost.com{race.attrib["href"]}')
                except (KeyError, IndexError, AttributeError):
                    continue

            job.log_message(f'Found {len(urls)} races so far from {race_date}')

        except Exception as e:
            job.log_message(f'Error fetching {url}: {str(e)}', 'error')
            continue

    return sorted(urls, key=lambda url: (url.split('/')[6], url.split('/')[5]))


def prepare_betfair(race_urls: list[str], paths: Paths, job: ScrapingJob):
    """Prepare Betfair data if available."""
    if not settings.toml or not settings.toml.get('betfair_data', False):
        return None

    try:
        from utils.betfair import Betfair

        if paths.betfair.exists():
            job.log_message('Using cached Betfair data')
            return Betfair.from_csv(paths.betfair)

        job.log_message('Fetching Betfair data...')
        betfair = Betfair(race_urls)

        with open(paths.betfair, 'w') as f:
            fields = settings.toml.get('fields', {}).get('betfair', {})
            header = ','.join(['date', 'region', 'off', 'horse'] + list(fields.keys()))
            f.write(header + '\n')

            for row in betfair.rows:
                values = ['' if v is None else str(v) for v in row.to_dict().values()]
                f.write(','.join(values) + '\n')

        job.log_message('Betfair data fetched successfully', 'success')
        return betfair

    except Exception as e:
        job.log_message(f'Error preparing Betfair data: {str(e)}', 'error')
        return None


def scrape_races(
    race_urls: list[str],
    paths: Paths,
    race_types: list[str],
    client: NetworkClient,
    file_writer,
    job: ScrapingJob,
):
    """Scrape races with progress tracking."""
    betfair = prepare_betfair(race_urls, paths, job)

    last_url = paths.progress.read_text().strip() if paths.progress.exists() else None

    if last_url:
        try:
            start_idx = race_urls.index(last_url) + 1
            race_urls = race_urls[start_idx:]
            job.log_message(f'Resuming after {last_url}')
        except ValueError:
            pass

    append = last_url is not None and paths.output.exists()

    total_races = len(race_urls)
    processed = 0
    skipped = 0

    with file_writer(str(paths.output), append=append) as f:
        if not append:
            f.write(settings.csv_header + '\n')

        for idx, url in enumerate(race_urls):
            if job.cancel_requested:
                raise Exception("Scraping cancelled by user")

            try:
                progress = (idx / total_races) * 100 if total_races > 0 else 0
                job.update_progress(
                    60 + (progress * 0.4),  # Start at 60%, go to 100%
                    f'Processing {idx + 1}/{total_races}: {url.split("/")[-1]}'
                )

                _, response = client.get(url)
                doc = html.fromstring(response.content)

                try:
                    race = (
                        Race(client, url, doc, settings.fields, betfair.data)
                        if betfair
                        else Race(client, url, doc, settings.fields)
                    )
                except VoidRaceError:
                    continue

                # Filter by race types
                if race_types and 'both' not in race_types:
                    # Check if this race type should be included
                    race_type_match = False
                    for race_type in race_types:
                        allowed_types = RACE_TYPES.get(race_type, set())
                        if race.race_info.race_type in allowed_types:
                            race_type_match = True
                            break
                    
                    if not race_type_match:
                        skipped += 1
                        continue

                for row in race.csv_data:
                    f.write(row + '\n')

                paths.progress.write_text(url)
                processed += 1

                if (idx + 1) % 10 == 0:
                    job.log_message(f'Processed {idx + 1}/{total_races} races')

            except VoidRaceError:
                continue
            except Exception as e:
                job.log_message(f'Error processing {url}: {str(e)}', 'error')
                continue

    job.update_progress(100, f'Finished scraping. Processed {processed}/{total_races} races (skipped {skipped}).', 'success')
    job.output_file = str(paths.output.resolve())


def writer_csv(file_path: str, append: bool = False):
    return open(file_path, 'a' if append else 'w', encoding='utf-8')


def writer_gzip(file_path: str, append: bool = False):
    mode = 'at' if append else 'wt'
    return gzip.open(file_path, mode, encoding='utf-8')


def scrape_worker(job: ScrapingJob):
    """Worker thread for scraping."""
    try:
        job.start_time = datetime.now().isoformat()
        job.status = "running"
        job.update_progress(0, "Initializing scraper...")

        # Initialize network client
        client = NetworkClient(
            email=job.config.get('email'),
            auth_state=job.config.get('auth_state'),
            access_token=job.config.get('access_token'),
        )

        job.log_message('Connected to Racing Post')
        
        # Get race types
        race_types_list = job.config.get('race_types', ['flat'])
        race_types_display = 'flat & jumps' if len(race_types_list) > 1 else race_types_list[0]
        job.update_progress(2, f"Mode: {job.config['mode']}, Race types: {race_types_display}")

        # Create RequestKey for build_paths
        mode = job.config['mode']
        scope_name = job.config.get('scope_name', 'custom')
        
        if mode == 'years':
            scope_value = '_'.join(job.config['years'])
        elif mode == 'dates':
            scope_value = '_'.join(str(d) for d in job.config['dates'])
        else:
            scope_value = scope_name
        
        request_key = RequestKey(
            scope_kind=mode,
            scope_value=scope_value[:50],  # Limit to 50 chars to avoid path issues
            race_type='both' if len(race_types_list) > 1 else race_types_list[0],
            filename='races'
        )

        # Build paths
        paths = build_paths(request_key, job.config['gzip'])

        if job.config['clean']:
            job.log_message('Clearing previous request data...')
            # Clear files
            for p in (paths.urls, paths.betfair, paths.progress, paths.output):
                if p.exists():
                    p.unlink()

        # Get race URLs
        job.update_progress(5, "Fetching race URLs...")

        if job.config['mode'] == 'years':
            # By years - can be all races or specific tracks
            if job.config.get('all_races'):
                job.log_message(f'🌍 Scraping ALL races ({race_types_display}) from selected years (all courses)')
                race_urls = get_all_races_by_years(
                    job.config['years'],
                    client,
                    job,
                )
            else:
                race_urls = get_race_urls(
                    job.config['years'],
                    job.config['tracks'],
                    race_types_list,
                    client,
                    job,
                )
        elif job.config['mode'] == 'dates':
            # By dates - can be all races or specific tracks
            if job.config.get('all_races'):
                job.log_message(f'🌍 Scraping ALL races ({race_types_display}) from selected dates (all courses)')
                race_urls = get_all_races_by_date(
                    job.config['dates'],
                    client,
                    job,
                )
            else:
                race_urls = get_race_urls_date(
                    job.config['dates'],
                    job.config['tracks'],
                    client,
                    job,
                )
        else:
            raise ValueError(f"Unknown mode: {mode}")

        job.log_message(f'Found {len(race_urls)} races to scrape', 'success')

        if not race_urls:
            job.log_message('No races found!', 'warning')
            job.update_progress(100, 'Completed - no races found', 'warning')
            job.status = "completed"
            return

        # Scrape races
        job.update_progress(60, f"Starting scrape of races ({race_types_display})...")
        file_writer = writer_gzip if job.config['gzip'] else writer_csv

        scrape_races(
            race_urls,
            paths,
            race_types_list,
            client,
            file_writer,
            job,
        )

        job.status = "completed"
        job.end_time = datetime.now().isoformat()

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.log_message(f'Fatal error: {str(e)}', 'error')
        job.end_time = datetime.now().isoformat()


# Routes

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'ok', 'version': '1.0.0'})


@app.route('/api/scrape/start', methods=['POST'])
def start_scrape():
    """Start a new scraping job."""
    try:
        data = request.get_json()

        # Validate input
        if not data.get('mode') in ['years', 'dates']:
            return jsonify({'error': 'Invalid mode'}), 400

        all_races = data.get('all_races', False)

        if not all_races and not data.get('tracks'):
            return jsonify({'error': 'No tracks specified'}), 400

        if data['mode'] == 'years' and not data.get('years'):
            return jsonify({'error': 'No years specified'}), 400

        if data['mode'] == 'dates' and not data.get('dates'):
            return jsonify({'error': 'No dates specified'}), 400

        # Get race types
        race_types = data.get('race_types', ['flat'])
        if isinstance(race_types, str):
            race_types = [race_types]

        # Parse dates if needed
        config = data.copy()
        if config['mode'] == 'dates':
            try:
                config['dates'] = [
                    datetime.strptime(d, '%Y-%m-%d').date() 
                    for d in config['dates']
                ]
            except ValueError as e:
                return jsonify({'error': f'Invalid date format: {str(e)}'}), 400

        config['race_types'] = race_types

        # Set scope name for all races
        if all_races:
            if config['mode'] == 'years':
                config['scope_name'] = f"all_races_{len(config['years'])}_years"
            else:
                config['scope_name'] = f"all_races_{len(config['dates'])}_dates"

        # Create job
        job_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        job = ScrapingJob(job_id, config)
        scraping_jobs[job_id] = job

        # Start scraping in background thread
        thread = threading.Thread(target=scrape_worker, args=(job,), daemon=True)
        thread.start()

        return jsonify({
            'job_id': job_id,
            'status': 'started',
        }), 202

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scrape/<job_id>', methods=['GET'])
def get_scrape_status(job_id: str):
    """Get status of a scraping job."""
    if job_id not in scraping_jobs:
        return jsonify({'error': 'Job not found'}), 404

    job = scraping_jobs[job_id]
    return jsonify(job.to_dict()), 200


@app.route('/api/scrape/<job_id>/cancel', methods=['POST'])
def cancel_scrape(job_id: str):
    """Cancel a scraping job."""
    if job_id not in scraping_jobs:
        return jsonify({'error': 'Job not found'}), 404

    job = scraping_jobs[job_id]
    job.cancel_requested = True
    job.log_message('Cancellation requested', 'warning')

    return jsonify({'status': 'cancelled'}), 200


@app.route('/api/scrape/<job_id>/download', methods=['GET'])
def download_output(job_id: str):
    """Download the output file for a completed job."""
    if job_id not in scraping_jobs:
        return jsonify({'error': 'Job not found'}), 404

    job = scraping_jobs[job_id]

    if job.status != 'completed':
        return jsonify({'error': 'Job not completed'}), 400

    if not job.output_file or not Path(job.output_file).exists():
        return jsonify({'error': 'Output file not found'}), 404

    return send_file(
        job.output_file,
        as_attachment=True,
        download_name=f'races_{job_id}.csv'
    )


@app.route('/api/jobs', methods=['GET'])
def list_jobs():
    """List all scraping jobs."""
    return jsonify({
        'jobs': [job.to_dict() for job in scraping_jobs.values()]
    }), 200


@app.route('/api/jobs/<job_id>', methods=['DELETE'])
def delete_job(job_id: str):
    """Delete a job from the list."""
    if job_id not in scraping_jobs:
        return jsonify({'error': 'Job not found'}), 404

    del scraping_jobs[job_id]
    return jsonify({'status': 'deleted'}), 200


@app.route('/api/logs', methods=['GET'])
def get_logs():
    """Get all logs from all jobs."""
    all_logs = []
    for job in scraping_jobs.values():
        all_logs.extend([
            {**log, 'job_id': job.job_id}
            for log in job.log
        ])
    return jsonify({'logs': all_logs}), 200


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)