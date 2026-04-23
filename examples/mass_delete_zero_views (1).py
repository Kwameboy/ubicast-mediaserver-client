#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Delete videos (and optionally lives) that have zero views over a given period.

HOW IT WORKS
------------
Choose a --delete-date in the future. Running the script BEFORE that date will
send warning emails to speakers (and optionally channel managers) about the
impending deletion. Running it ON or AFTER the delete date will actually delete
the media.  Use the same filters on every run so the selection stays consistent.

Workflow
--------
1. Pick a --delete-date far enough ahead for users to react (e.g. 4 weeks out).
2. Run the script (dry run first, then --apply) to send warning emails.
3. Re-run periodically as reminders until the delete date.
4. After the delete date, run once more with --apply to perform the deletion.

Usage examples
--------------
Dry run — see what would happen:
    python mass_delete_zero_views.py --conf acc.json \\
        --views-after 2022-01-01 --views-before 2024-12-31 \\
        --delete-date 2025-06-01 --fallback-email admin@example.com

Send warning emails (before delete-date):
    python mass_delete_zero_views.py --conf acc.json \\
        --views-after 2022-01-01 --views-before 2024-12-31 \\
        --delete-date 2025-06-01 --fallback-email admin@example.com --apply

Actually delete (on or after delete-date):
    python mass_delete_zero_views.py --conf acc.json \\
        --views-after 2022-01-01 --views-before 2024-12-31 \\
        --delete-date 2025-06-01 --fallback-email admin@example.com --apply

Test your email template without sending anything:
    python mass_delete_zero_views.py --conf acc.json \\
        --views-after 2022-01-01 --views-before 2024-12-31 \\
        --delete-date 2025-06-01 --fallback-email admin@example.com \\
        --test-email-template

Skip media protected by a category:
    python mass_delete_zero_views.py --conf acc.json \\
        --views-after 2022-01-01 --views-before 2024-12-31 \\
        --delete-date 2025-06-01 --fallback-email admin@example.com \\
        --skip-category "do not delete" --skip-category "archive" --apply
'''

import argparse
import csv
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import zip_longest
import logging
import os
from pathlib import Path
import smtplib
import ssl
import sys
import threading
from urllib.parse import urlparse


try:
    from ms_client.client import MediaServerClient
except ModuleNotFoundError:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from ms_client.client import MediaServerClient


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Email templates
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Warning email — shown BEFORE the delete date
# ---------------------------------------------------------------------------

DEFAULT_PLAIN_WARNING_TEMPLATE = (
    'Beste collega,\n\n'
    'Op het UU-videoplatform ({platform_hostname}) heb je beeldmateriaal staan dat weinig tot niet meer '
    'wordt bekeken. Dit materiaal is geselecteerd voor automatische verwijdering. '
    'Onder aan deze e-mail vind je een overzicht van de betreffende opnames.\n\n'
    'Wat moet je doen?\n'
    'We vragen je om te controleren of het beeldmateriaal verwijderd kan worden, of dat je het wilt '
    'behouden. Wil je de opnames behouden? Volg dan de instructies in de handleiding om het materiaal '
    'te bewaren. Als je geen actie onderneemt, wordt het beeldmateriaal op {delete_date} automatisch '
    'naar de prullenbak verplaatst.\n'
    'Je kunt het materiaal bewaren door de categorie "{skip_categories}" in te stellen via de '
    'bewerklink naast elk bestand hieronder.\n\n'
    'Bewaartermijn\n'
    'Meer informatie over de bewaartermijnen en de achtergrond van dit beleid kun je vinden op:\n'
    'https://intranet.uu.nl/kennisbank/het-uu-videoplatform\n\n'
    'Hulp nodig of overleg?\n'
    'Neem contact op met de key-user van het videoplatform binnen jouw afdeling.\n\n'
    'Overzicht van te beoordelen mediabestanden ({media_count} bestand(en), totaal {media_size_pp}):\n'
    '{list_of_media}\n\n'
    'Met vriendelijke groet,\nUU-Videoplatform\nUniversiteit Utrecht\n\n'
    '---\n\n'
    'Dear colleague,\n\n'
    'You have video content on the UU Video Platform ({platform_hostname}) that is rarely or no longer '
    'viewed. This content has been selected for automatic deletion. At the bottom of this email, you '
    'will find a list of the recordings concerned.\n\n'
    'What do you need to do?\n'
    'We ask that you check whether the video content can be deleted or if you would like to keep it. '
    'Want to keep the recordings? Follow the instructions in the manual to preserve the material. '
    'If no action is taken, the video content will be automatically moved to the recycle bin on '
    '{delete_date}.\n'
    'You can protect content by setting the category "{skip_categories}" using the edit link next to '
    'each file below.\n\n'
    'Retention periods\n'
    'More information: https://intranet.uu.nl/en/knowledgebase/the-uu-video-platform\n\n'
    'Need help or want to discuss?\n'
    'Please contact the key-user for the video platform within your department.\n\n'
    'Overview of media to review ({media_count} file(s), total {media_size_pp}):\n'
    '{list_of_media}\n\n'
    'Kind regards,\nUU Video Platform\nUtrecht University\n\n'
    '---\n'
    'Dit is een automatisch gegenereerd bericht. Gelieve niet te antwoorden. / '
    'This is an automatically generated message. Please do not reply.\n'
)

# The HTML warning template is loaded from email_zero_views_warning.html if it exists,
# otherwise this minimal fallback is used.
DEFAULT_HTML_WARNING_TEMPLATE = (
    '<p>Beste collega / Dear colleague,</p>'
    '<p>De volgende {media_count} mediabestanden (totaal {media_size_pp}) gehost op '
    '{platform_hostname} zijn geselecteerd voor verwijdering op <strong>{delete_date}</strong>. '
    'Stel de categorie &ldquo;{skip_categories}&rdquo; in om bestanden te bewaren. / '
    'The following {media_count} file(s) (total {media_size_pp}) on {platform_hostname} are '
    'scheduled for deletion on <strong>{delete_date}</strong>. Set the category '
    '&ldquo;{skip_categories}&rdquo; to preserve files.</p>'
    '<ul>{list_of_media}</ul>'
    '<p>Met vriendelijke groet / Kind regards,<br><strong>UU Video Platform</strong></p>'
    '<p><small>Dit is een automatisch gegenereerd bericht. Gelieve niet te antwoorden. / '
    'This is an automatically generated message. Please do not reply.</small></p>'
)

# ---------------------------------------------------------------------------
# Deletion confirmation email — shown AFTER media are deleted
# ---------------------------------------------------------------------------

DEFAULT_PLAIN_DELETED_TEMPLATE = (
    'Beste collega,\n\n'
    'Onlangs heb je een bericht ontvangen over jouw beeldmateriaal op het UU-videoplatform '
    '({platform_hostname}) dat weinig tot niet meer werd bekeken. Hieronder vind je een overzicht '
    'van het betreffende materiaal.\n\n'
    'Materiaal verwijderd\n'
    'Het beeldmateriaal is inmiddels verwijderd en verplaatst naar de prullenbak. Het blijft daar '
    'nog maximaal één jaar beschikbaar om eventueel teruggehaald te worden. Je ontvangt hierover '
    'verder geen herinneringen.\n\n'
    'Bewaartermijn\n'
    'Meer informatie: https://intranet.uu.nl/kennisbank/het-uu-videoplatform\n\n'
    'Hulp of overleg\n'
    'Heb je hulp nodig of wil je materiaal terughalen uit de prullenbak? Neem dan contact op met '
    'de key-user van het videoplatform binnen jouw afdeling.\n\n'
    'Overzicht van verwijderde mediabestanden ({media_count} bestand(en), totaal {media_size_pp}):\n'
    '{list_of_media}\n\n'
    'Met vriendelijke groet,\nUU-Videoplatform\nUniversiteit Utrecht\n\n'
    '---\n\n'
    'Dear colleague,\n\n'
    'You recently received a notification regarding your video content on the UU Video Platform '
    '({platform_hostname}) that was rarely or no longer being viewed. Below, you will find a list '
    'of the recordings concerned.\n\n'
    'Content Deleted\n'
    'The video content has now been deleted and moved to the recycle bin. It will remain there for '
    'up to one year, during which time it can still be recovered. You will no longer receive '
    'reminders about this.\n\n'
    'Retention periods\n'
    'More information: https://intranet.uu.nl/en/knowledgebase/the-uu-video-platform\n\n'
    'Need help or want to discuss?\n'
    'If you need help or would like to recover content from the recycle bin, please contact the '
    'key-user for the video platform within your department.\n\n'
    'Overview of deleted media ({media_count} file(s), total {media_size_pp}):\n'
    '{list_of_media}\n\n'
    'Kind regards,\nUU Video Platform\nUtrecht University\n\n'
    '---\n'
    'Dit is een automatisch gegenereerd bericht. Gelieve niet te antwoorden. / '
    'This is an automatically generated message. Please do not reply.\n'
)

DEFAULT_HTML_DELETED_TEMPLATE = (
    '<p>Beste collega / Dear colleague,</p>'
    '<p>Het beeldmateriaal hieronder is verwijderd en naar de prullenbak verplaatst. '
    'Het blijft maximaal één jaar beschikbaar. / '
    'The content below has been deleted and moved to the recycle bin. '
    'It remains available for up to one year.</p>'
    '<ul>{list_of_media}</ul>'
    '<p>Met vriendelijke groet / Kind regards,<br><strong>UU Video Platform</strong></p>'
    '<p><small>Dit is een automatisch gegenereerd bericht. Gelieve niet te antwoorden. / '
    'This is an automatically generated message. Please do not reply.</small></p>'
)

# Dummy data used by --test-email-template
DUMMY_MEDIAS = [
    {
        'oid': 'oid_1',
        'title': 'Lecture recording — Introduction to Python',
        'add_date': '2023-09-01 10:00:00',
        'storage_used': 5 * 1_000_000_000,
        'views_over_period': 0,
        'views_after': '2022-01-01',
        'views_before': '2024-12-31',
        'speaker_id': '',
        'speaker_email': 'speaker1@example.com',
        'channel_title': 'CS101',
        'faculty_title': 'Faculty of Science',
        'managers_emails': '',
    },
    {
        'oid': 'oid_2',
        'title': 'Guest lecture — Data Structures',
        'add_date': '2022-03-15 14:30:00',
        'storage_used': 8 * 1_000_000_000,
        'views_over_period': 0,
        'views_after': '2022-01-01',
        'views_before': '2024-12-31',
        'speaker_id': '',
        'speaker_email': 'speaker2@example.com',
        'channel_title': 'CS201',
        'faculty_title': 'Faculty of Science',
        'managers_emails': '',
    },
]

# ---------------------------------------------------------------------------
# Thread-local MediaServerClient
# ---------------------------------------------------------------------------

_thread_local = threading.local()


def _get_thread_client(conf_path):
    if not hasattr(_thread_local, 'msc'):
        _thread_local.msc = MediaServerClient(conf_path, setup_logging=False)
    return _thread_local.msc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_bytes(num_bytes: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if num_bytes < 1000:
            return f'{num_bytes:.1f} {unit}'
        num_bytes /= 1000
    return f'{num_bytes:.1f} PB'


# ---------------------------------------------------------------------------
# Media selection
# ---------------------------------------------------------------------------

def _get_zero_view_medias(
    msc: MediaServerClient,
    views_after: date,
    views_before: date,
    views_playback_threshold: int = 0,
    added_after: date = None,
    added_before: date = None,
    skip_categories: set = frozenset(),
    include_lives: bool = False,
) -> list:
    '''
    Return a list of media dicts that have 0 views in the given period.
    Each dict is enriched with:
      - views_over_period, views_after, views_before
      - channel_title, faculty_title
      - managers_emails  (from parent channel, for fallback email routing)
    '''
    logger.info('Fetching full catalog...')
    catalog = msc.get_catalog('flat')

    logger.info('Querying stats/unwatched/ for zero-view media...')
    # views_threshold=1 so the endpoint returns media with views_over_period < 1
    # (i.e. exactly 0 views). The API uses strict less-than internally, so
    # passing 0 would return nothing — bug confirmed & fixed by leverancier.
    # We then filter for views_over_period == 0 as an extra safety check.
    unwatched_response = msc.api(
        'stats/unwatched/',
        params={
            'playback_threshold': views_playback_threshold,
            'views_threshold': 1,
            'recursive': 'yes',
            'sd': views_after.strftime('%Y-%m-%d'),
            'ed': views_before.strftime('%Y-%m-%d'),
        },
    )
    zero_view_oids = {
        item['object_id']: item['views_over_period']
        for item in unwatched_response['unwatched']
        if item['views_over_period'] == 0  # only truly zero-view media
    }
    logger.info(f'Stats endpoint returned {len(zero_view_oids)} zero-view media.')

    # Build channel lookups
    channels = {ch['oid']: ch for ch in catalog.get('channels', [])}

    # Map each channel oid -> faculty title (level-1 ancestor)
    faculty_of = {}
    for ch in catalog.get('channels', []):
        oid = ch['oid']
        parent = ch.get('parent_oid')
        if not parent:
            faculty_of[oid] = ch.get('title', oid)
        else:
            grandparent = channels.get(parent, {}).get('parent_oid')
            if not grandparent:
                faculty_of[oid] = channels.get(parent, {}).get('title', parent)
            else:
                faculty_of[oid] = channels.get(grandparent, {}).get('title', grandparent)

    media_types = ['videos']
    if include_lives:
        media_types.append('lives')

    selected = []
    for key in media_types:
        for media in catalog.get(key, []):
            oid = media['oid']
            if oid not in zero_view_oids:
                continue

            # Date filters
            add_date = datetime.strptime(media['add_date'], '%Y-%m-%d %H:%M:%S').date()
            if added_after and add_date < added_after:
                logger.debug(f'{oid} skipped: added before {added_after}.')
                continue
            if added_before and add_date >= added_before:
                logger.debug(f'{oid} skipped: added on/after {added_before}.')
                continue

            # Category filter
            categories = {
                cat.strip(' \r\t').lower()
                for cat in (media.get('categories') or '').strip('\n').split('\n')
            }
            if skip_categories and categories.intersection(skip_categories):
                logger.debug(f'{oid} skipped: has protected category.')
                continue

            parent_oid = media.get('parent_oid', '')
            parent_ch = channels.get(parent_oid, {})
            media['views_over_period'] = zero_view_oids[oid]
            media['views_after'] = views_after.strftime('%Y-%m-%d')
            media['views_before'] = views_before.strftime('%Y-%m-%d')
            media['channel_title'] = parent_ch.get('title', '')
            media['faculty_title'] = faculty_of.get(parent_oid, '')
            media['managers_emails'] = parent_ch.get('managers_emails') or ''
            selected.append(media)

    total_bytes = sum(m.get('storage_used', 0) for m in selected)
    logger.info(
        f'{len(selected)} media selected '
        f'(total size: {_format_bytes(total_bytes)}).'
    )
    return selected


# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------

def _get_templates(html_template_path: Path, plain_template_path: Path, mode: str = 'warning'):
    '''
    Load email templates from disk; fall back to built-in UU bilingual defaults.
    mode: 'warning'  — email sent before deletion (action required)
          'deleted'  — email sent after deletion (confirmation)
    '''
    default_html = DEFAULT_HTML_WARNING_TEMPLATE if mode == 'warning' else DEFAULT_HTML_DELETED_TEMPLATE
    default_plain = DEFAULT_PLAIN_WARNING_TEMPLATE if mode == 'warning' else DEFAULT_PLAIN_DELETED_TEMPLATE

    try:
        html_template = html_template_path.read_text(encoding='utf-8')
        html_is_custom = True
    except FileNotFoundError:
        html_template = default_html
        html_is_custom = False

    try:
        plain_template = plain_template_path.read_text(encoding='utf-8')
        plain_is_custom = True
    except FileNotFoundError:
        plain_template = default_plain
        plain_is_custom = False

    if html_is_custom and not plain_is_custom:
        plain_template = None
        logger.info(f'[{mode}] Using custom HTML template only (no plain template found).')
    elif not html_is_custom and plain_is_custom:
        html_template = None
        logger.info(f'[{mode}] Using custom plain template only (no HTML template found).')
    elif not html_is_custom and not plain_is_custom:
        logger.info(f'[{mode}] Using built-in default UU bilingual templates.')
    else:
        logger.info(f'[{mode}] Using custom HTML and plain email templates.')

    return html_template, plain_template


def _get_users(msc: MediaServerClient, page_size: int = 500) -> list:
    users = []
    offset = 0
    response = msc.api('users/', params={'limit': page_size, 'offset': offset})
    while response['users']:
        users += response['users']
        offset += page_size
        response = msc.api('users/', params={'limit': page_size, 'offset': offset})
    return users


def _build_mail(
    msc: MediaServerClient,
    sender: str,
    recipient_email: str,
    medias: list,
    delete_date: date,
    skip_categories: set,
    html_template,
    plain_template,
    subject_template: str,
) -> str:
    '''Build and return a MIME email as a string.'''
    # De-duplicate by oid
    medias = list({m['oid']: m for m in medias}.values())

    server_url = msc.conf['SERVER_URL'].rstrip('/')
    perma_base = f'{server_url}/permalink/'
    edit_base = f'{server_url}/edit/iframe/'

    context = {
        'media_count': len(medias),
        'media_size_pp': _format_bytes(sum(m.get('storage_used', 0) for m in medias)),
        'delete_date': delete_date.strftime('%B %d, %Y'),
        'skip_categories': ' | '.join(f'"{cat}"' for cat in sorted(skip_categories)),
        'platform_hostname': urlparse(msc.conf['SERVER_URL']).netloc,
    }

    message = MIMEMultipart('alternative')
    message['Subject'] = subject_template.format(**context)
    message['From'] = sender
    message['To'] = recipient_email

    now = datetime.now()
    media_contexts = []
    for media in medias:
        add_dt = datetime.strptime(media['add_date'], '%Y-%m-%d %H:%M:%S')
        age_days = (now - add_dt).days
        media_contexts.append({
            'title': media.get('title', ''),
            'add_date': add_dt.strftime('%Y-%m-%d'),
            'age_days': age_days,
            'view_url': f'{perma_base}{media["oid"]}/',
            'edit_url': f'{edit_base}{media["oid"]}/#id_categories',
            'views_after': media.get('views_after', ''),
            'views_before': media.get('views_before', ''),
        })

    if plain_template:
        plain_list = '\n'.join(
            (
                '\t- {view_url} - "{title}" - toegevoegd op / added on {add_date} ({age_days} dagen/days ago), '
                '0 weergaven/views between {views_after} and {views_before} '
                '(bewaren/protect: {edit_url})'
            ).format(**ctx)
            for ctx in media_contexts
        )
        message.attach(MIMEText(plain_template.format(list_of_media=plain_list, **context), 'plain'))

    if html_template:
        # Build table rows — compatible with both warning and deletion HTML templates
        html_rows = '\n'.join(
            (
                '<tr>'
                '<td><a href="{view_url}">{title}</a></td>'
                '<td>{add_date} ({age_days} dagen/days)</td>'
                '<td><a href="{edit_url}">Bewaren / Protect</a></td>'
                '</tr>'
            ).format(**ctx)
            for ctx in media_contexts
        )
        message.attach(MIMEText(html_template.format(list_of_media=html_rows, **context), 'html'))

    return message.as_string()


# ---------------------------------------------------------------------------
# Email sending
# ---------------------------------------------------------------------------

def _warn_speakers(
    msc: MediaServerClient,
    medias: list,
    delete_date: date,
    skip_categories: set,
    html_template_path: Path,
    plain_template_path: Path,
    subject_template: str,
    fallback_to_channel_manager: bool,
    fallback_email: str,
    apply: bool = False,
):
    '''
    Send warning emails to speakers (and optionally channel managers) about
    the impending deletion of their zero-view media.
    '''
    smtp_server = msc.conf.get('SMTP_SERVER')
    smtp_login = msc.conf.get('SMTP_LOGIN')
    smtp_password = msc.conf.get('SMTP_PASSWORD')
    smtp_email = msc.conf.get('SMTP_SENDER_EMAIL')
    if not all((smtp_server, smtp_login, smtp_password, smtp_email)):
        raise RuntimeError(
            'SMTP settings incomplete in config. '
            'Need: SMTP_SERVER, SMTP_LOGIN, SMTP_PASSWORD, SMTP_SENDER_EMAIL. '
            f'Got: {smtp_server=}, {smtp_login=}, '
            f'SMTP_PASSWORD={"set" if smtp_password else "missing"}, {smtp_email=}'
        )

    html_template, plain_template = _get_templates(html_template_path, plain_template_path, mode='warning')

    # Build valid-email lookup from the user list
    logger.info('Fetching user list...')
    users = _get_users(msc)
    valid_emails = {
        (user.get('email') or '').strip().lower()
        for user in users
        if user.get('is_active') and (user.get('email') or '').strip()
    }
    speaker_id_to_email = {}
    for user in users:
        sid = (user.get('speaker_id') or '').strip()
        email = (user.get('email') or '').strip().lower()
        if sid and email and user.get('is_active'):
            speaker_id_to_email[sid] = email

    # Group media per recipient
    medias_per_recipient: dict = {}
    to_fallback = []

    for media in medias:
        recipients = []
        speaker_ids = [s.strip() for s in (media.get('speaker_id') or '').split('|') if s.strip()]
        speaker_emails = [s.strip().lower() for s in (media.get('speaker_email') or '').split('|') if s.strip()]

        for speaker_id, speaker_email in zip_longest(speaker_ids, speaker_emails):
            if speaker_email and speaker_email in valid_emails:
                recipients.append(speaker_email)
            elif speaker_id and speaker_id_to_email.get(speaker_id) in valid_emails:
                recipients.append(speaker_id_to_email[speaker_id])

        if not recipients and fallback_to_channel_manager:
            for mgr_email in (media.get('managers_emails') or '').split('\n'):
                mgr_email = mgr_email.strip(' \r\t').lower()
                if mgr_email and not mgr_email.startswith('#') and mgr_email in valid_emails:
                    recipients.append(mgr_email)

        if not recipients:
            to_fallback.append(media)
        else:
            for r in recipients:
                medias_per_recipient.setdefault(r, []).append(media)

    # Build all messages upfront
    to_send = {
        recipient: _build_mail(
            msc,
            sender=smtp_email,
            recipient_email=recipient,
            medias=recipient_medias,
            delete_date=delete_date,
            skip_categories=skip_categories,
            html_template=html_template,
            plain_template=plain_template,
            subject_template=subject_template,
        )
        for recipient, recipient_medias in medias_per_recipient.items()
    }

    smtp_ctx = (
        smtplib.SMTP_SSL(smtp_server, 465, context=ssl.create_default_context())
        if apply else nullcontext()
    )
    sent_count = 0

    with smtp_ctx as smtp:
        if apply:
            smtp.login(smtp_login, smtp_password)

        for recipient, message in to_send.items():
            try:
                if apply:
                    smtp.sendmail(smtp_email, recipient, message)
                logger.debug(
                    f'{"Sent" if apply else "[Dry run] Would send"} warning email to {recipient}.'
                )
                sent_count += 1
            except smtplib.SMTPException as err:
                logger.error(f'Failed to send email to {recipient}: {err}. Adding to fallback.')
                to_fallback += medias_per_recipient[recipient]

        # Always send a fallback email (covers unroutable media + delivery failures)
        if to_fallback or not medias_per_recipient:
            fallback_message = _build_mail(
                msc,
                sender=smtp_email,
                recipient_email=fallback_email,
                medias=to_fallback if to_fallback else medias,
                delete_date=delete_date,
                skip_categories=skip_categories,
                html_template=html_template,
                plain_template=plain_template,
                subject_template=subject_template,
            )
            try:
                if apply:
                    smtp.sendmail(smtp_email, fallback_email, fallback_message)
                logger.debug(
                    f'{"Sent" if apply else "[Dry run] Would send"} '
                    f'fallback warning email to {fallback_email}.'
                )
                sent_count += 1
            except Exception as err:
                logger.error(f'Failed to deliver fallback email to {fallback_email}: {err}')
                raise

    if apply:
        logger.info(f'Sent {sent_count} warning email(s).')
    else:
        logger.info(f'[Dry run] {sent_count} warning email(s) would have been sent.')


def _send_deletion_confirmation(
    msc: MediaServerClient,
    medias: list,
    delete_results: dict,
    html_template_path: Path,
    plain_template_path: Path,
    subject_template: str,
    fallback_to_channel_manager: bool,
    fallback_email: str,
    apply: bool = False,
):
    '''
    Send a deletion confirmation email to speakers after media have been deleted,
    listing only the successfully deleted items per recipient.
    '''
    smtp_server = msc.conf.get('SMTP_SERVER')
    smtp_login = msc.conf.get('SMTP_LOGIN')
    smtp_password = msc.conf.get('SMTP_PASSWORD')
    smtp_email = msc.conf.get('SMTP_SENDER_EMAIL')
    if not all((smtp_server, smtp_login, smtp_password, smtp_email)):
        raise RuntimeError(
            'SMTP settings incomplete in config. '
            'Need: SMTP_SERVER, SMTP_LOGIN, SMTP_PASSWORD, SMTP_SENDER_EMAIL.'
        )

    html_template, plain_template = _get_templates(html_template_path, plain_template_path, mode='deleted')

    # Only include successfully deleted media
    deleted_medias = [m for m in medias if delete_results.get(m['oid']) is True]
    if not deleted_medias:
        logger.info('No successfully deleted media to notify about.')
        return

    logger.info('Fetching user list for deletion confirmation emails...')
    users = _get_users(msc)
    valid_emails = {
        (user.get('email') or '').strip().lower()
        for user in users
        if user.get('is_active') and (user.get('email') or '').strip()
    }
    speaker_id_to_email = {}
    for user in users:
        sid = (user.get('speaker_id') or '').strip()
        email = (user.get('email') or '').strip().lower()
        if sid and email and user.get('is_active'):
            speaker_id_to_email[sid] = email

    medias_per_recipient: dict = {}
    to_fallback = []

    for media in deleted_medias:
        recipients = []
        speaker_ids = [s.strip() for s in (media.get('speaker_id') or '').split('|') if s.strip()]
        speaker_emails = [s.strip().lower() for s in (media.get('speaker_email') or '').split('|') if s.strip()]

        for speaker_id, speaker_email in zip_longest(speaker_ids, speaker_emails):
            if speaker_email and speaker_email in valid_emails:
                recipients.append(speaker_email)
            elif speaker_id and speaker_id_to_email.get(speaker_id) in valid_emails:
                recipients.append(speaker_id_to_email[speaker_id])

        if not recipients and fallback_to_channel_manager:
            for mgr_email in (media.get('managers_emails') or '').split('\n'):
                mgr_email = mgr_email.strip(' \r\t').lower()
                if mgr_email and not mgr_email.startswith('#') and mgr_email in valid_emails:
                    recipients.append(mgr_email)

        if not recipients:
            to_fallback.append(media)
        else:
            for r in recipients:
                medias_per_recipient.setdefault(r, []).append(media)

    # Use a neutral delete_date (today) since deletion already happened
    today_date = date.today()
    skip_categories_placeholder = set()

    to_send = {
        recipient: _build_mail(
            msc,
            sender=smtp_email,
            recipient_email=recipient,
            medias=recipient_medias,
            delete_date=today_date,
            skip_categories=skip_categories_placeholder,
            html_template=html_template,
            plain_template=plain_template,
            subject_template=subject_template,
        )
        for recipient, recipient_medias in medias_per_recipient.items()
    }

    smtp_ctx = (
        smtplib.SMTP_SSL(smtp_server, 465, context=ssl.create_default_context())
        if apply else nullcontext()
    )
    sent_count = 0

    with smtp_ctx as smtp:
        if apply:
            smtp.login(smtp_login, smtp_password)

        for recipient, message in to_send.items():
            try:
                if apply:
                    smtp.sendmail(smtp_email, recipient, message)
                logger.debug(
                    f'{"Sent" if apply else "[Dry run] Would send"} '
                    f'deletion confirmation to {recipient}.'
                )
                sent_count += 1
            except smtplib.SMTPException as err:
                logger.error(f'Failed to send deletion confirmation to {recipient}: {err}.')
                to_fallback += medias_per_recipient[recipient]

        if to_fallback or not medias_per_recipient:
            fallback_message = _build_mail(
                msc,
                sender=smtp_email,
                recipient_email=fallback_email,
                medias=to_fallback if to_fallback else deleted_medias,
                delete_date=today_date,
                skip_categories=skip_categories_placeholder,
                html_template=html_template,
                plain_template=plain_template,
                subject_template=subject_template,
            )
            try:
                if apply:
                    smtp.sendmail(smtp_email, fallback_email, fallback_message)
                logger.debug(
                    f'{"Sent" if apply else "[Dry run] Would send"} '
                    f'fallback deletion confirmation to {fallback_email}.'
                )
                sent_count += 1
            except Exception as err:
                logger.error(f'Failed to deliver fallback deletion confirmation to {fallback_email}: {err}')
                raise

    if apply:
        logger.info(f'Sent {sent_count} deletion confirmation email(s).')
    else:
        logger.info(f'[Dry run] {sent_count} deletion confirmation email(s) would have been sent.')


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------

def _delete_medias(msc: MediaServerClient, medias: list, apply: bool = False) -> dict:
    '''
    Delete medias via catalog/bulk_delete/.
    Returns dict  oid -> True (deleted) | False (error).
    '''
    results = {}
    server_url = msc.conf['SERVER_URL'].rstrip('/')

    if not medias:
        logger.info('No media to delete.')
        return results

    oids = [m['oid'] for m in medias]
    total_bytes = sum(m.get('storage_used', 0) for m in medias)

    if not apply:
        for oid in oids:
            logger.debug(f'[Dry run] Would delete {server_url}/permalink/{oid}/')
            results[oid] = True
        logger.info(
            f'[Dry run] {len(oids)} media ({_format_bytes(total_bytes)}) '
            f'would have been deleted.'
        )
        return results

    response = msc.api(
        'catalog/bulk_delete/',
        method='post',
        data=dict(oids=oids),
    )
    media_by_oid = {m['oid']: m for m in medias}
    deleted_count = 0
    deleted_bytes = 0
    for oid, status in response.get('statuses', {}).items():
        if status.get('status') == 200:
            results[oid] = True
            deleted_count += 1
            deleted_bytes += media_by_oid.get(oid, {}).get('storage_used', 0)
            logger.debug(f'Deleted {server_url}/permalink/{oid}/')
        else:
            results[oid] = False
            logger.error(
                f'Failed to delete {server_url}/permalink/{oid}/: '
                f'{status.get("message", "unknown error")}'
            )

    logger.info(
        f'{deleted_count}/{len(oids)} media deleted '
        f'({_format_bytes(deleted_bytes)} freed).'
    )
    return results


# ---------------------------------------------------------------------------
# Report & summary
# ---------------------------------------------------------------------------

def _write_report(report_path: str, medias: list, delete_results: dict, server_url: str, dry_run: bool):
    server_url = server_url.rstrip('/')
    fieldnames = [
        'status', 'faculty', 'channel', 'oid', 'title',
        'add_date', 'storage_gb', 'views_after', 'views_before', 'link',
    ]
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for media in medias:
            oid = media['oid']
            if dry_run:
                status = 'would_delete'
            else:
                outcome = delete_results.get(oid)
                status = 'deleted' if outcome is True else ('error' if outcome is False else 'not_attempted')
            writer.writerow({
                'status': status,
                'faculty': media.get('faculty_title', ''),
                'channel': media.get('channel_title', ''),
                'oid': oid,
                'title': media.get('title', ''),
                'add_date': media.get('add_date', ''),
                'storage_gb': f'{media.get("storage_used", 0) / 1_000_000_000:.3f}',
                'views_after': media.get('views_after', ''),
                'views_before': media.get('views_before', ''),
                'link': f'{server_url}/permalink/{oid}/',
            })
    logger.info(f'Report written to: {report_path}')


def _write_summary(summary_path: str, medias: list, delete_results: dict, dry_run: bool):
    faculty_stats = {}
    for media in medias:
        faculty = media.get('faculty_title') or '(no faculty)'
        s = faculty_stats.setdefault(faculty, {'total': 0, 'deleted': 0, 'error': 0, 'bytes': 0})
        s['total'] += 1
        s['bytes'] += media.get('storage_used', 0)
        outcome = delete_results.get(media['oid'])
        if outcome is True:
            s['deleted'] += 1
        elif outcome is False:
            s['error'] += 1

    lines = []
    grand_total = grand_deleted = grand_bytes = 0
    for faculty in sorted(faculty_stats):
        s = faculty_stats[faculty]
        grand_total += s['total']
        grand_deleted += s['deleted']
        grand_bytes += s['bytes']
        lines.append(faculty)
        if dry_run:
            lines.append(f"  Would delete: {s['total']} media  ({_format_bytes(s['bytes'])})")
        else:
            lines.append(
                f"  Deleted: {s['deleted']}/{s['total']}  "
                f"errors: {s['error']}  ({_format_bytes(s['bytes'])})"
            )

    lines.append('')
    if dry_run:
        lines.append(
            f'TOTAL (dry run): {grand_total} media  ({_format_bytes(grand_bytes)}) '
            f'would be deleted. No changes were made.'
        )
    else:
        lines.append(
            f'TOTAL: {grand_deleted}/{grand_total} media deleted  '
            f'({_format_bytes(grand_bytes)} freed).'
        )

    summary_text = '\n'.join(lines)
    print('\n' + summary_text)
    Path(summary_path).write_text(summary_text + '\n', encoding='utf-8')
    logger.info(f'Summary written to: {summary_path}')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=__doc__.strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--conf',
        default='acc.json',
        help='Path to the MediaServer configuration file (default: acc.json).',
        type=str,
    )
    parser.add_argument(
        '--delete-date',
        help='Date on/after which media will be deleted, e.g. "2025-06-01". '
             'Before this date the script sends warning emails instead. Required.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--fallback-email',
        help='Email address that receives warnings for media with no identifiable speaker '
             'or channel manager. Also receives a copy when individual delivery fails. Required.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--views-after',
        help='Start date of the view-count period, e.g. "2022-01-01". Required.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--views-before',
        help='End date of the view-count period, e.g. "2024-12-31". '
             'Must be at least yesterday. Required.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--views-playback-threshold',
        help='Minimum playback time in seconds to count as a view (default: 0).',
        type=int,
        default=0,
    )
    parser.add_argument(
        '--added-after',
        help='Only consider media added on or after this date, e.g. "2020-01-01".',
        type=str,
        required=False,
    )
    parser.add_argument(
        '--added-before',
        help='Only consider media added strictly before this date, e.g. "2024-01-01".',
        type=str,
        required=False,
    )
    parser.add_argument(
        '--skip-category',
        help='Skip media that have this category. Repeatable. '
             'Example: --skip-category "do not delete" --skip-category "archive".',
        dest='skip_categories',
        action='append',
        default=[],
    )
    parser.add_argument(
        '--include-lives',
        help='Also consider live recordings (default: videos only).',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--fallback-to-channel-manager',
        help='If a media has no identifiable speaker, send the warning to the channel manager.',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--send-email-on-deletion',
        help='Also send warning emails on the deletion run (on/after --delete-date).',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--html-email-template',
        help='Path to a custom HTML warning email template (sent before deletion). '
             'Defaults to ./email_zero_views_warning.html. '
             'Variables: platform_hostname, media_count, media_size_pp, '
             'delete_date, skip_categories, list_of_media.',
        type=Path,
        default=Path('./email_zero_views_warning.html'),
    )
    parser.add_argument(
        '--plain-email-template',
        help='Path to a custom plain-text warning email template (same variables as HTML template). '
             'Defaults to ./email_zero_views_warning.txt.',
        type=Path,
        default=Path('./email_zero_views_warning.txt'),
    )
    parser.add_argument(
        '--html-deleted-template',
        help='Path to a custom HTML deletion confirmation email template (sent after deletion). '
             'Defaults to ./email_zero_views_deleted.html.',
        type=Path,
        default=Path('./email_zero_views_deleted.html'),
    )
    parser.add_argument(
        '--plain-deleted-template',
        help='Path to a custom plain-text deletion confirmation email template. '
             'Defaults to ./email_zero_views_deleted.txt.',
        type=Path,
        default=Path('./email_zero_views_deleted.txt'),
    )
    parser.add_argument(
        '--email-subject-template',
        help='Template string for the WARNING email subject line.',
        type=str,
        default=(
            'Actie vereist / Action required — {platform_hostname}: '
            '{media_count} mediabestanden worden verwijderd op / media files will be deleted on {delete_date}'
        ),
    )
    parser.add_argument(
        '--deleted-subject-template',
        help='Template string for the DELETION CONFIRMATION email subject line.',
        type=str,
        default=(
            'Beeldmateriaal verwijderd / Content deleted — {platform_hostname}: '
            '{media_count} mediabestand(en) verplaatst naar prullenbak / file(s) moved to recycle bin'
        ),
    )
    parser.add_argument(
        '--send-deletion-confirmation',
        help='Send a deletion confirmation email to speakers after media are deleted.',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--test-email-template',
        help='Print a sample email to the console using dummy data. '
             'No emails are sent and no media are deleted.',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--report',
        default=None,
        help='Path to the output report CSV. '
             'Defaults to delete_zero_views_<conf_stem>_<timestamp>.csv',
        type=str,
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Apply changes. Without this flag the script is a dry run.',
    )
    parser.add_argument(
        '--log-level',
        default='info',
        choices=['critical', 'error', 'warn', 'info', 'debug'],
        help='Log level (default: info).',
    )
    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(message)s')
    logger.setLevel(args.log_level.upper())

    conf_stem = Path(args.conf).stem
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = args.report or f'delete_zero_views_{conf_stem}_{timestamp}.csv'
    summary_path = f'delete_zero_views_summary_{conf_stem}.txt'

    # Parse & validate dates
    today = date.today()
    safe_end = today - timedelta(days=1)
    delete_date = datetime.strptime(args.delete_date, '%Y-%m-%d').date()
    views_after = datetime.strptime(args.views_after, '%Y-%m-%d').date()
    views_before = datetime.strptime(args.views_before, '%Y-%m-%d').date()

    if views_before > safe_end:
        print(
            f'Error: --views-before must be at most {safe_end} (yesterday) '
            'to ensure stats are fully computed.'
        )
        sys.exit(1)
    if views_after >= views_before:
        print('Error: --views-after must be earlier than --views-before.')
        sys.exit(1)

    added_after = datetime.strptime(args.added_after, '%Y-%m-%d').date() if args.added_after else None
    added_before = datetime.strptime(args.added_before, '%Y-%m-%d').date() if args.added_before else None
    skip_categories = {cat.strip().lower() for cat in args.skip_categories} or {'do not delete'}

    # Connect
    msc = MediaServerClient(args.conf)
    print(msc.api('/'))
    msc.conf['TIMEOUT'] = max(600, msc.conf.get('TIMEOUT', 60))

    # --test-email-template: render a sample email and exit
    if args.test_email_template:
        html_template, plain_template = _get_templates(
            args.html_email_template, args.plain_email_template
        )
        message = _build_mail(
            msc,
            sender=msc.conf.get('SMTP_SENDER_EMAIL', 'noreply@example.com'),
            recipient_email=args.fallback_email,
            medias=DUMMY_MEDIAS,
            delete_date=delete_date,
            skip_categories=skip_categories,
            html_template=html_template,
            plain_template=plain_template,
            subject_template=args.email_subject_template,
        )
        print(message)
        sys.exit(0)

    dry_run = not args.apply
    if dry_run:
        logger.info('[Dry run] No emails will be sent and no media will be deleted. Use --apply to act.')
    else:
        if today >= delete_date:
            prompt = (
                f'Running in APPLY mode against {msc.conf["SERVER_URL"]}.\n'
                f'Media with 0 views WILL BE DELETED (moved to recycle-bin if enabled).\n'
                f'Verify the recycle-bin is enabled: '
                f'{msc.conf["SERVER_URL"]}/admin/settings/#id_trash_enabled\n'
                f'Proceed? [y/n] '
            )
        else:
            prompt = (
                f'Running in APPLY mode against {msc.conf["SERVER_URL"]}.\n'
                f'Warning emails WILL BE SENT to speakers. Deletion scheduled for {delete_date}.\n'
                f'Proceed? [y/n] '
            )
        if input(prompt).strip().lower() not in ('y', 'yes'):
            print('Aborted.')
            sys.exit(0)

    # Select media
    medias = _get_zero_view_medias(
        msc,
        views_after=views_after,
        views_before=views_before,
        views_playback_threshold=args.views_playback_threshold,
        added_after=added_after,
        added_before=added_before,
        skip_categories=skip_categories,
        include_lives=args.include_lives,
    )

    if not medias:
        print('No media matched the given filters. Nothing to do.')
        sys.exit(0)

    # Before delete date → warn speakers
    if today < delete_date or args.send_email_on_deletion:
        _warn_speakers(
            msc,
            medias,
            delete_date=delete_date,
            skip_categories=skip_categories,
            html_template_path=args.html_email_template,
            plain_template_path=args.plain_email_template,
            subject_template=args.email_subject_template,
            fallback_to_channel_manager=args.fallback_to_channel_manager,
            fallback_email=args.fallback_email,
            apply=args.apply,
        )

    # On/after delete date → delete
    delete_results = {}
    if today >= delete_date:
        delete_results = _delete_medias(msc, medias, apply=args.apply)
        # Send deletion confirmation emails if requested
        if args.send_deletion_confirmation:
            _send_deletion_confirmation(
                msc,
                medias,
                delete_results=delete_results,
                html_template_path=args.html_deleted_template,
                plain_template_path=args.plain_deleted_template,
                subject_template=args.deleted_subject_template,
                fallback_to_channel_manager=args.fallback_to_channel_manager,
                fallback_email=args.fallback_email,
                apply=args.apply,
            )
    else:
        logger.info(
            f'Delete date ({delete_date}) has not passed yet — skipping deletion. '
            f'Run again on or after {delete_date} with --apply to delete.'
        )

    # Always write report & summary (useful even on warning-only runs)
    _write_report(report_path, medias, delete_results, msc.conf['SERVER_URL'], dry_run)
    _write_summary(summary_path, medias, delete_results, dry_run)


if __name__ == '__main__':
    main()
