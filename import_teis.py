#!/usr/bin/env python3
"""
Imports "Quarantine Site" tracked entity instances as "facility"
CommCare cases.

IDs are specific to the staging.dhis.hisp.org server.

The script requires the following environment variables to be set:

* DHIS2_USERNAME
* DHIS2_PASSWORD
* COMMCARE_USERNAME
* COMMCARE_PASSWORD

"""
import os
import sys
from contextlib import contextmanager
from tempfile import TemporaryFile
from typing import Iterable

import requests
import tablib

DHIS2_BASE_URL = 'https://staging.dhis.hisp.org/sl-idsr/'
DHIS2_PAGE_SIZE = 50
COMMCARE_BASE_URL = 'https://www.commcarehq.org/'
COMMCARE_PROJECT_SPACE = 'sl-demo'
COMMCARE_CASE_TYPE = 'quarantine_facility'

# Map DHIS2 tracked entity attributes and JSON properties to CommCare
# case properties.
CASE_PROPERTY_MAP = {
    # DHIS2 TEI property (as given in JSON): CommCare case property

    # NOTE: external_id is set to DHIS2 UID, not Site ID
    'trackedEntityInstance': 'external_id',

    # NOTE: DHIS2 orgUnit maps to CommCare owner; Cases are owned by
    #       their district.
    'orgUnit': 'owner_id',

    'attributes': {
        'X0UVSJM0r8Y': (  # Address
            'quarantine_facility_name',
            'address',
        ),
        'XY2SluAX1nC': 'mobile_number',  # Mobile No 1
        'dOafaJ8AVoj': 'start_date',  # Start date
        'weWd2HBcwzK': {  # Type
            'case_property': 'quarantine_facility_type',
            'value_map': {
                'QS_TYPE_SELF': 'self',
                'QS_TYPE_GOV': 'gov',
                'QS_TYPE_ORG': 'org',
            }
        },

        'dwgKrnlpXL0': 'dhis2_number_in_quarantine',  # Number in quarantine
        'J1ZmgfnCXxA': 'dhis2_num_chld_under_5',  # Children under 5 years
        'mCliML3JzL0': 'dhis2_num_chld_self_fac',  # Children 5-19 years
        'ZRhH6eyz7Oz': 'dhis2_num_adults_self_fac',  # Adults 20+ years
        'YUAMGTtigwP': 'dhis2_name',  # Name
        'BqfnZJKOkhB': 'dhis2_manager',  # Manager
        'K8i9ZfrGkt1': 'dhis2_district',  # District
        'hyymts7JjMp': 'dhis2_site_id',  # Site ID
        'SmW3apOMu3E': 'dhis2_zone',  # Chiefdom/Zone/City
        'AtcW78731s8': 'dhis2_remarks',  # Remarks
    }
}

# Map DHIS2 organisation units to CommCare locations.
ORG_UNIT_MAP = {
    # DHIS2 OrgUnit ID: CommCare location ID
    # 'Plmg8ikyfrK': 'not-a-real-location-id',  # Sierra Leone
    'zTHjIvPmPI0': 'b8c3c4514b9d4f2e8057ef51e14c3736',  # Bo District
    'ucyuSTA19cP': '0d028b468f584d7e98908036fecc5bf8',  # Bombali District
    'iQgaTATK59f': '635b027b51f1497b85b79623f6903372',  # Bonthe District
    'pWb43ue4nt8': '031d11add3bd4426a405b2c6f82f946f',  # Falaba District
    'oWaJJJLCWzI': '79af11227ba74f25acca227fdffc543c',  # Kailahun District
    'Ba6McOxy6D2': '1f7a65aec4dc4f209bca50356620fda9',  # Kambia District
    'x4tRoN2ue9w': 'b692666cf2994a2aad85363b30e36568',  # Karene District
    'gheAIXur8EJ': '175024c160474ac6aaa84a070a850d8d',  # Kenema District
    'Y7YrGOsu9fp': 'b069228c635a4885aa3c105cc6eabf6c',  # Koinadugu District
    'P7LlNb9MosU': 'e16a8b8b93f7488b9c2eceb5f6ffad8d',  # Kono District
    'btW1NRETLww': '74a29d76d0dc490f80269b09dbc8416a',  # Moyamba District
    'RjOUVJDV1Dl': '9709ddb2cfa9412ba4f0bf1ec73cd392',  # Port Loko District
    'ywV0ByvR7xW': 'badc81c939f54523a22668f79408b1e1',  # Pujehun District
    'yA18sXAwZJo': '3aa2d8127850415fa66b06e9b2b9afc2',  # Tonkolili District
    'knrO5gazmom': '96f0546c6c9243279ea4e7366c753d59',  # Western Area Rural District
    'PMLlCzM0mWT': 'e8654ba06c0f4339a92c8bb99f4c2481',  # Western Area Urban District
}

# If a TEI has an organisation unit that is not mapped to a CommCare
# owner, should an error be raised? If set to `False`, the TEI will
# be skipped.
RAISE_ERROR_ON_MISSING_ORG_UNIT = True


def get_name(tracked_entity) -> str:
    """
    Returns the site's address, which CommCare uses as the case's name.
    If the address is not given, transforms the site name.

    e.g. ::

        >>> tracked_entity = {
        ...     'trackedEntityInstance': 'iHhCKKXHQv6',
        ...     'orgUnit': 'O6uvpzGd5pu',
        ...     'attributes': [{
        ...         'attribute': 'YUAMGTtigwP',  # Name
        ...         'value': 'QSS_5_EXAMPLE_RD_MURRAY_TOWN'
        ...     },{
        ...         'attribute': 'X0UVSJM0r8Y',  # Address
        ...         'value': '5 Example Road, Murray Town'
        ...     }]
        ... }
        >>> get_name(tracked_entity)
        '5 Example Road, Murray Town'
        >>> tracked_entity = {
        ...     'trackedEntityInstance': 'iHhCKKXHQv6',
        ...     'orgUnit': 'O6uvpzGd5pu',
        ...     'attributes': [{
        ...         'attribute': 'YUAMGTtigwP',  # Name
        ...         'value': 'QSS_5_EXAMPLE_RD_MURRAY_TOWN'
        ...     }]
        ... }
        >>> get_name(tracked_entity)
        '5 Example Rd Murray Town'

    """
    attrs = tracked_entity['attributes']
    address = [a['value'] for a in attrs if a['attribute'] == 'X0UVSJM0r8Y']
    name = [a['value'] for a in attrs if a['attribute'] == 'YUAMGTtigwP']
    if address and address[0]:
        return address[0]
    elif name and name[0] and len(name[0] > 4):
        without_prefix = name[0][4:]
        underscores_replaced = without_prefix.replace('_', ' ')
        return underscores_replaced.title()
    else:
        return 'Address unknown'


def get_tracked_entities_from_dhis2() -> Iterable[dict]:
    """
    Returns an iterable of dictionaries, as returned by the DHIS2 web
    API. (Can be a list or a generator. A generator could be useful if
    the function handles pagination.)
    """
    endpoint = '/api/trackedEntityInstances'
    url = prefix_base_url(DHIS2_BASE_URL, endpoint)
    params = {
        # 'trackedEntityType': 'qtKp6wnGc1L',  # Site
        'program': 'MjFF9x9Cka9',  # Quarantine Site Daily Monitoring
        'paging': 'True',
        'pageSize': DHIS2_PAGE_SIZE,
    }
    headers = {'Accept': 'application/json'}
    auth = (os.environ['DHIS2_USERNAME'], os.environ['DHIS2_PASSWORD'])
    for ou in ORG_UNIT_MAP:
        params.update({
            'ou': ou,
            'page': 1,
        })
        while True:
            response = requests.get(url, params, headers=headers, auth=auth)
            teis = response.json()['trackedEntityInstances']
            for tei in teis:
                yield tei
            if len(teis) < DHIS2_PAGE_SIZE:
                # The "trackedEntityInstances" endpoint does not give us
                # paging data like some other endpoints. We know we're
                # on the last page if we didn't get a full page of
                # results.
                break
            params['page'] += 1


def map_tracked_entity_attributes(tracked_entities) -> Iterable[dict]:
    """
    Takes an iterable of tracked entities, and returns an interable of
    dictionaries with tracked entity attributes mapped to case property
    values.
    """
    for tracked_entity in tracked_entities:
        dhis2_id_property = CASE_PROPERTY_MAP['trackedEntityInstance']
        org_unit_property = CASE_PROPERTY_MAP['orgUnit']

        name = get_name(tracked_entity)
        tei_id = tracked_entity['trackedEntityInstance']
        org_unit = tracked_entity['orgUnit']
        if org_unit not in ORG_UNIT_MAP:
            if RAISE_ERROR_ON_MISSING_ORG_UNIT:
                raise KeyError(
                    'CommCare location cannot be determined for organisation '
                    f'unit {org_unit!r}. Please add the organisation unit to '
                    '`ORG_UNIT_MAP`, or set `RAISE_ERROR_ON_MISSING_ORG_UNIT '
                    '= False`.'
                )
            else:
                print(f'Skipping TEI {tei_id!r} ({name}): Unknown org unit '
                      f'{org_unit!r}')
                continue
        commcare_location = ORG_UNIT_MAP[org_unit]

        case_properties = {
            'name': name,
            dhis2_id_property: tei_id,
            org_unit_property: commcare_location,
        }
        for attr in tracked_entity['attributes']:
            if attr['attribute'] in CASE_PROPERTY_MAP['attributes']:
                dhis2_id = attr['attribute']
                case_property = CASE_PROPERTY_MAP['attributes'][dhis2_id]
                dict_ = get_case_property_values(case_property, attr['value'])
                case_properties.update(dict_)
        yield case_properties


def get_case_property_values(case_property, dhis2_value) -> dict:
    """
    Returns a dictionary of case property names and values

    >>> get_case_property_values('order', 'spam')
    {'order': 'spam'}
    >>> get_case_property_values({
    ...     'case_property': 'order',
    ...     'value_map': {'bacon': 'spam'}
    ... }, 'bacon')
    {'order': 'spam'}
    >>> get_case_property_values(('order', 'menu'), 'spam')
    {'order': 'spam', 'menu': 'spam'}

    """
    case_properties = {}
    if isinstance(case_property, (list, tuple)):
        for p in case_property:
            case_properties.update(get_case_property_values(p, dhis2_value))
    elif isinstance(case_property, str):
        case_properties[case_property] = dhis2_value
    elif isinstance(case_property, dict):
        property_name = case_property['case_property']
        mapped_value = case_property['value_map'][dhis2_value]
        case_properties[property_name] = mapped_value
    return case_properties


@contextmanager
def save_cases(cases):
    """
    Saves cases to a temporary file. Returns the file object as a
    context object. Deletes the file after it has been used.
    """
    attribute_case_property_names = [
        name for value in CASE_PROPERTY_MAP['attributes'].values()
        for name in get_property_names(value)
    ]
    other_case_property_names = [
        name for key, value in CASE_PROPERTY_MAP.items() if key != 'attributes'
        for name in get_property_names(value)
    ]
    headers = [
        'name',
        *other_case_property_names,
        *attribute_case_property_names,
    ]
    data = tablib.Dataset(headers=headers)
    for case in cases:
        data.append([case.get(k) for k in headers])
    with TemporaryFile() as tempfile:
        excel_data = data.export('xlsx')
        tempfile.write(excel_data)
        tempfile.seek(0)
        yield tempfile


def get_property_names(case_property) -> list:
    """
    Returns a list of case property names from a CASE_PROPERTY_MAP value

    >>> get_property_names('order')
    ['order']
    >>> get_property_names({'case_property': 'order'})
    ['order']
    >>> get_property_names(('order', 'menu'))
    ['order', 'menu']
    """
    if isinstance(case_property, (list, tuple)):
        return [n for p in case_property for n in get_property_names(p)]
    if isinstance(case_property, str):
        return [case_property]
    if isinstance(case_property, dict):
        return [case_property['case_property']]


def bulk_upload_cases(tempfile):
    """
    Uploads case data stored in ``tempfile`` to CommCare HQ. Returns a
    status URL if upload succeeds. Raises an exception if upload fails.
    """
    endpoint = f'/a/{COMMCARE_PROJECT_SPACE}/importer/excel/bulk_upload_api/'
    url = prefix_base_url(COMMCARE_BASE_URL, endpoint)
    data = {
        'case_type': COMMCARE_CASE_TYPE,
        'search_field': 'external_id',
        'create_new_cases': 'on',
        'name_column': 'name',
        'comment': 'Imported from DHIS2 tracked entities',
    }
    files = {'file': (f'{COMMCARE_CASE_TYPE}_cases.xlsx', tempfile)}
    auth = (os.environ['COMMCARE_USERNAME'], os.environ['COMMCARE_PASSWORD'])
    response = requests.post(url, data, files=files, auth=auth)
    response.raise_for_status()
    return response.json()['status_url']


def prefix_base_url(base_url, endpoint):
    """
    Returns ``base_url`` + ``endpoint`` with the right forward slashes.

    >>> prefix_base_url('https://play.dhis2.org/dev/',
    ...                 '/api/trackedEntityInstances')
    'https://play.dhis2.org/dev/api/trackedEntityInstances'

    >>> prefix_base_url('https://play.dhis2.org/dev',
    ...                 'api/trackedEntityInstances')
    'https://play.dhis2.org/dev/api/trackedEntityInstances'

    """
    return '/'.join((base_url.rstrip('/'), endpoint.lstrip('/')))


def get_missing_env_vars():
    env_vars = (
        'DHIS2_USERNAME',
        'DHIS2_PASSWORD',
        'COMMCARE_USERNAME',
        'COMMCARE_PASSWORD',
    )
    return [v for v in env_vars if v not in os.environ]


if __name__ == '__main__':
    missing = get_missing_env_vars()
    if missing:
        print('These required environment variables are not set:',
              ', '.join(missing))
        sys.exit(1)

    tracked_entities = get_tracked_entities_from_dhis2()
    cases = map_tracked_entity_attributes(tracked_entities)
    with save_cases(cases) as tempfile:
        status_url = bulk_upload_cases(tempfile)
    print('Upload successful. Import in progress.')
    print(f'Poll {status_url} for progress updates')
