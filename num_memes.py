if __name__ == '__main__':
    import json
    from collections import Counter
    with open('memes/scraped.json', 'r') as f:
        memes = json.loads(f.read())
    with open('memes/settings.txt', 'r') as f:
        settings = json.loads(f.read())
    thresholds = settings.get('threshold_upvotes')
    total, postable = Counter(), Counter()
    for post, data in memes.items():
        if not data.get('over_18'):
            sub = data.get('sub', '').lower()
            ups = data.get('highest_ups')
            sub_threshold = thresholds.get(sub, thresholds['global'])

            total[sub] += 1
            if ups >= sub_threshold:
                postable[sub] += 1
    print(json.dumps(total, indent=2))
    print(json.dumps(postable, indent=2))

    print()
    print('total: {:,}'.format(sum(total.values())))
    print('postable: {:,}'.format(sum(postable.values())))
