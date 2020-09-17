import requests


def get_my_ip():
    ip = requests.get('https://api.ipify.org').text
    return ip


def get_filters(filters):
    return [{'Name': k, 'Values': [v]} for k, v in filters.items()]


def get_tag(tag, resource_type=None):
    if tag is None or len(tag) == 0:
        return []

    tags = [
        {
            'Key': str(tag[0]),
            'Value': str(tag[1])
        },
    ]

    if resource_type is None:
        return tags
    else:
        return [
            {
                'ResourceType': resource_type,
                'Tags': tags
            },
        ]


def get_list_param(param):
    if param is None:
        return []
    elif isinstance(param, str):
        return [param]
    else:
        return param


def get_next_token(response):
    if response is None:
        return ''
    elif 'NextToken' in response:
        return response['NextToken']
    else:
        return ''
