import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    path = event.get('path', '')
    
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS'
    }
    
    if method == 'OPTIONS':
        return response(200, {}, headers)
    
    try:
        if path == '/api/health' and method == 'GET':
            return response(200, {'success': True, 'message': 'Music API server is running'}, headers)
        
        elif path == '/api/login' and method == 'POST':
            return handle_login(event, headers)
        
        elif path == '/api/register' and method == 'POST':
            return handle_register(event, headers)
        
        elif path == '/api/music/query' and method == 'GET':
            return handle_query(event, headers)
        
        elif path == '/api/subscriptions' and method == 'GET':
            return handle_get_subscriptions(event, headers)
        
        elif path == '/api/subscriptions' and method == 'POST':
            return handle_subscribe(event, headers)
        
        elif path == '/api/subscriptions' and method == 'DELETE':
            return handle_remove_subscription(event, headers)
        
        else:
            return response(404, {'success': False, 'message': 'Not found'}, headers)
    
    except Exception as e:
        return response(500, {'success': False, 'message': str(e)}, headers)

def handle_login(event, headers):
    body = json.loads(event.get('body', '{}'))
    email = body.get('email', '')
    password = body.get('password', '')
    
    table = dynamodb.Table('login')
    result = table.get_item(Key={'email': email})
    item = result.get('Item')
    
    if item and item.get('password') == password:
        return response(200, {
            'success': True,
            'email': email,
            'user_name': item.get('user_name')
        }, headers)
    else:
        return response(200, {
            'success': False,
            'message': 'email or password is invalid'
        }, headers)

def handle_register(event, headers):
    body = json.loads(event.get('body', '{}'))
    email = body.get('email', '')
    user_name = body.get('user_name', '')
    password = body.get('password', '')
    
    table = dynamodb.Table('login')
    result = table.get_item(Key={'email': email})
    
    if result.get('Item'):
        return response(200, {
            'success': False,
            'message': 'The email already exists'
        }, headers)
    
    table.put_item(Item={
        'email': email,
        'user_name': user_name,
        'password': password
    })
    
    return response(200, {
        'success': True,
        'email': email,
        'user_name': user_name
    }, headers)

def handle_query(event, headers):
    params = event.get('queryStringParameters') or {}
    title = params.get('title', '').strip().lower()
    year = params.get('year', '').strip()
    artist = params.get('artist', '').strip().lower()
    album = params.get('album', '').strip().lower()
    
    table = dynamodb.Table('music')
    
    if artist and not album and not title and not year:
        result = table.query(
            KeyConditionExpression=Key('artist').eq(
                find_exact_artist(artist)
            )
        )
        songs = result.get('Items', [])
    elif album:
        result = table.query(
            IndexName='album-artist-index',
            KeyConditionExpression=Key('album').eq(
                find_exact_album(album)
            )
        )
        songs = result.get('Items', [])
        if artist:
            songs = [s for s in songs if s.get('artist', '').lower() == artist]
        if title:
            songs = [s for s in songs if title in s.get('title', '').lower()]
        if year:
            songs = [s for s in songs if s.get('year', '') == year]
    else:
        result = table.scan()
        songs = result.get('Items', [])
        if artist:
            songs = [s for s in songs if s.get('artist', '').lower() == artist]
        if title:
            songs = [s for s in songs if title in s.get('title', '').lower()]
        if year:
            songs = [s for s in songs if s.get('year', '') == year]
        if album:
            songs = [s for s in songs if album in s.get('album', '').lower()]
    
    formatted = [format_song(s) for s in songs]
    
    if not formatted:
        return response(200, {
            'success': False,
            'message': 'No result is retrieved. Please query again',
            'songs': []
        }, headers)
    
    return response(200, {
        'success': True,
        'songs': formatted
    }, headers)

def handle_get_subscriptions(event, headers):
    params = event.get('queryStringParameters') or {}
    email = params.get('email', '')
    
    table = dynamodb.Table('subscriptions')
    result = table.query(
        KeyConditionExpression=Key('email').eq(email)
    )
    
    subs = result.get('Items', [])
    music_table = dynamodb.Table('music')
    
    formatted = []
    for sub in subs:
        song_result = music_table.get_item(
            Key={
                'artist': sub.get('artist'),
                'song_id': sub.get('song_id')
            }
        )
        song = song_result.get('Item')
        if song:
            formatted.append(format_song(song))
    
    return response(200, {
        'success': True,
        'subscriptions': formatted
    }, headers)

def handle_subscribe(event, headers):
    body = json.loads(event.get('body', '{}'))
    email = body.get('email', '')
    artist = body.get('artist', '')
    song_id = body.get('song_id', '')
    
    table = dynamodb.Table('subscriptions')
    
    try:
        table.put_item(
            Item={
                'email': email,
                'song_id': song_id,
                'artist': artist
            },
            ConditionExpression='attribute_not_exists(email) AND attribute_not_exists(song_id)'
        )
        return response(200, {'success': True, 'message': 'Subscribed successfully'}, headers)
    except Exception:
        return response(200, {'success': False, 'message': 'Already subscribed'}, headers)

def handle_remove_subscription(event, headers):
    params = event.get('queryStringParameters') or {}
    email = params.get('email', '')
    song_id = params.get('song_id', '')
    
    table = dynamodb.Table('subscriptions')
    table.delete_item(Key={'email': email, 'song_id': song_id})
    
    return response(200, {'success': True, 'message': 'Removed successfully'}, headers)

def format_song(song):
    artist = song.get('artist', '')
    image_key = f"artist-images/{artist.replace(' ', '')}.jpg"
    s3_url = f"https://s4135523-music-images.s3.amazonaws.com/{image_key}"
    
    return {
        'song_id': song.get('song_id', ''),
        'title': song.get('title', ''),
        'artist': artist,
        'year': song.get('year', ''),
        'album': song.get('album', ''),
        'image_url': song.get('image_url', ''),
        's3_image_url': s3_url
    }

def find_exact_artist(artist_lower):
    table = dynamodb.Table('music')
    result = table.scan()
    for item in result.get('Items', []):
        if item.get('artist', '').lower() == artist_lower:
            return item.get('artist')
    return artist_lower

def find_exact_album(album_lower):
    table = dynamodb.Table('music')
    result = table.scan()
    for item in result.get('Items', []):
        if item.get('album', '').lower() == album_lower:
            return item.get('album')
    return album_lower

def response(status_code, body, headers):
    return {
        'statusCode': status_code,
        'headers': headers,
        'body': json.dumps(body)
    }