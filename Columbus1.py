import os, math, csv, json, string, psycopg2, urllib, requests, rauth, foursquare


#takes a place's GLP ID and returns a baseline rating by combining the ratings on google, fousquare and yelp
def baseline(glp_item_id): 
	conn = psycopg2.connect(
		dbname = os.environ['AWS_DBNAME'], 
		host = os.environ['AWS_HOST'], 
		port = os.environ['AWS_PORT'], 
		user = os.environ['AWS_USER'], 
		password = os.environ['AWS_PASSWORD'])
	cur = conn.cursor()
	cur.execute("SELECT name,foursquare_id, ST_AsText(lonlat) AS coords FROM glp_items WHERE id = %s" % glp_item_id)
	info = cur.fetchone()
	flag = 'approve'

	#if name in list of chains, flag it
	name = info[0].lower()
	rdr = csv.reader(open('chain list.csv','rb'))
	for rows in rdr:
		for i in range(len(rows)):
			if name == rows[i].lower():
				flag += ', chain'

	#take the first word in a place's name which isn't 'the' to be used as a keyword. also get rid of any annoying punctuation
	if 'the ' in name:
		name = name.replace('the ', '')
	name = name.translate(None, ''.join(set(string.punctuation)))
	name = name.partition(' ')[0]

	#take the lonlat object from the database and transform it into a latitude and longitude separated by a comma
	lonlat = str(info[2])[6:-1].split()
	latlon = lonlat[1] + ',' + lonlat[0]
	
	#GET THE GOOGLE RATING AND THE NUMBER OF RATINGS FROM THEIR API
	url1 = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=%s&radius=500&name=%s&key=%s" % (latlon, name, os.environ['GOOGLE_KEY'])
	response1 = urllib.urlopen(url1)
	data1 = json.loads(response1.read())
	try:
		reference = data1["results"][0]["reference"] #get the place's google reference
	except KeyError: google_rating = [0,0]
	except IndexError: google_rating = [0,0]
	try:
		url2 = "https://maps.googleapis.com/maps/api/place/details/json?reference=%s&key=%s" % (reference, os.environ['GOOGLE_KEY'])
		response2 = urllib.urlopen(url2)
		data2 = json.loads(response2.read())
		google_rating = [data1["results"][0]["rating"], data2["result"]["user_ratings_total"]] #get the rating and number of ratings
	except KeyError: google_rating = [0,0]
	except IndexError: google_rating = [0,0]
	except UnboundLocalError: google_rating = [0,0]

	#GET THE FOURSQUARE RATING AND THE NUMBER OF LIKES FROM THEIR API
	foursquare_client = foursquare.Foursquare(client_id = os.environ['FOURSQUARE_CLIENT_ID'], client_secret = os.environ['FOURSQUARE_CLIENT_SECRET'])
	try :
		data = foursquare_client.venues(info[1])
		foursquare_rating = [data['venue']['rating'], data['venue']['likes']['count']] #get the rating and the number of likes
	except foursquare.ParamError: foursquare_rating = [0,0]
	except foursquare.EndpointError: foursquare_rating = [0,0]
	except KeyError: foursquare_rating = [0,0]

	#GET THE YELP RATING AND THE NUMBER OF RATINGS FROM THEIR API
	params = {} #set up the search parameters and the session
	params["term"] = name
	params["ll"] = latlon
	params["radius_filter"] = "500"
	params["limit"] = "1"
	session = rauth.OAuth1Session(
		consumer_key = os.environ['YELP_CONSUMER_KEY'],
		consumer_secret = os.environ['YELP_CONSUMER_SECRET'],
		access_token = os.environ['YELP_ACCESS_TOKEN'],
		access_token_secret = os.environ['YELP_ACCESS_TOKEN_SECRET'])
	response = session.get("http://api.yelp.com/v2/search",params=params)
	data = json.loads(response.content)
	session.close()
	try:
		yelp_rating = [data['businesses'][0]['rating'], data['businesses'][0]['review_count']] #get the rating and number of ratings
	except IndexError: 	yelp_rating = [0,0]
	except KeyError: 	yelp_rating = [0,0]

	#return a normalised weighted average of the results 
	tot = google_rating[1] + foursquare_rating[1] + yelp_rating[1]
	g = google_rating[0] * google_rating[1] * 20
	f = foursquare_rating[0] * foursquare_rating[1] * 10
	y = yelp_rating[0] * yelp_rating[1] * 20
	if tot > 0:
		rating = (g + f + y)/tot
		uncertainty = 1/math.sqrt(tot)
	if tot <= 0: 
		rating = 75
		uncertainty = 1.0
		flag = 'couldn\'t scrape ratings'
	if uncertainty >= 0.32 and tot <= 0: flag = 'couldn\'t scrape ratings'
	if uncertainty >= 0.32 and tot > 0: flag = 'dis'+flag+', uncertain'
	if rating < 70: flag = 'dis'+flag
	return [rating, uncertainty, flag]


#goes through all actions in the last month, assigns each one to a place and then produces a distribution of scores for each of the behaviours
def produce_distributions():
	conn = psycopg2.connect(
		dbname = os.environ['AWS_DBNAME'], 
		host = os.environ['AWS_HOST'], 
		port = os.environ['AWS_PORT'], 
		user = os.environ['AWS_USER'], 
		password = os.environ['AWS_PASSWORD'])
	cur = conn.cursor()
	cur.execute("SELECT glp_item_id, action_type FROM user_actions WHERE actioned_at > date_trunc('day', NOW() - INTERVAL '1 month')")
	actions = cur.fetchall()
	
	cur.execute("SELECT id FROM glp_items")
	places = [int(str(place)[1:-2]) for place in cur.fetchall()] #get all place IDs

	#set up a dictionary to count the number of times various actions are executed on each place
	counters = {place : {'openings':0, 'times_seen':0, 'BBs':0, 'photo_scrolls':0} for place in places}
	for action in actions:
		if action[0] in counters:
			if action[1] == 1: counters[action[0]]['openings'] += 1
			if action[1] == 2 or action[1] == 1: counters[action[0]]['times_seen'] += 1
			if action[1] == 3 or action[1] == 4: counters[action[0]]['BBs'] += 1
			if action[1] == 10: counters[action[0]]['photo_scrolls'] += 1

	#produce the distributions of each type of score as a dictionary. this means each value is assigned to its 
	bb_distribution, opening_distribution, scroll_distribution = {}, {}, {}
	for place in places:
		if counters[place]['times_seen'] != 0:
			opening_distribution[place] = counters[place]['openings']/float(counters[place]['times_seen'])
		
		else: opening_distribution[place] = None

		if counters[place]['openings'] != 0:
			bb_distribution[place] = counters[place]['BBs']/float(counters[place]['openings'])
			scroll_distribution[place] = counters[place]['photo_scrolls']/float(counters[place]['openings'])
		
		else:
			bb_distribution[place] = None
			scroll_distribution[place] = None

	return [bb_distribution, opening_distribution, scroll_distribution, counters]	


#takes a place's GLP ID and returns a GLP score, based on the user interactions with that place compared to all other places
def produce_glp_score(glp_item_id, distributions):
	bb_distr, opening_distr, scroll_distr = distributions[0], distributions[1], distributions[2]
	places = list(set(place for place in bb_distr))

	sorted_bbs = sorted([bb_distr[place] for place in places])
	sorted_openings = sorted([opening_distr[place] for place in places])
	sorted_scrolls = sorted([scroll_distr[place] for place in places])

	try:
		bb_position = sorted_bbs.index(bb_distr[glp_item_id])
		opening_position = sorted_openings.index(opening_distr[glp_item_id])
		scroll_position = sorted_scrolls.index(scroll_distr[glp_item_id])

		bb_score = ((2.1*bb_position)/len(bb_distr))-1
		opening_score = ((2.1*opening_position)/len(opening_distr))-1
		scroll_score = ((2.1*scroll_position)/len(scroll_distr))-1
	except KeyError: return '\tCouldn\'t do this one! It seems to be missing from the database!'

	try: black_book_uncertainty = 20 * 1/math.sqrt(distributions[3][glp_item_id]['BBs'])
	except ZeroDivisionError: black_book_uncertainty = 1
	try: opening_uncertainty = 15 * 1/math.sqrt(distributions[3][glp_item_id]['openings'])
	except ZeroDivisionError: opening_uncertainty = 1
	try: photo_scroll_uncertainty = 5 * 1/math.sqrt(distributions[3][glp_item_id]['photo_scrolls'])
	except ZeroDivisionError: photo_scroll_uncertainty = 1

	#combine factors
	W, fw1, fw2, fw3 = (0,)*4
	if bb_score != -1 and bb_score is not None: 
		fw1 = bb_score * 20
		W += 20
	if opening_score != -1 and opening_score is not None:
		fw2 = opening_score * 15
		W += 15
	if scroll_score != -1 and scroll_score is not None:
		fw3 = scroll_score * 5
		W += 5

	if W == 0: 
		rating, uncertainty = 0, 0
	else:
		rating = (fw1 + fw2 + fw3) / float(W)
		uncertainty = math.sqrt(black_book_uncertainty**2 + opening_uncertainty**2 + photo_scroll_uncertainty**2)/float(W)

	if rating >= 0: flag = 'positive'
	if rating < 0: flag = 'negative'
	if uncertainty >= 0.32: flag+=', uncertain'
	if W == 0: return [0, 0, 'not enough actions to produce a reasonable augmentation']
	else: return [rating, uncertainty, flag]


#takes a place's baseline rating (/100) and it's GLP score (/1) and combines them to give the overall rating (/100)
def combine(baseline, glp_score):
	if baseline[0] != 0.0:
		rating = baseline[0] + 20*glp_score[0]
		if rating > 100: rating = 100
		if rating < 0: rating = 0
		uncertainty = math.sqrt(baseline[1]**2 + glp_score[1]**2) 
		return [rating, uncertainty]
	elif baseline[0] == 0.0:
		return 75 + 25*glp_score[0]


#takes a date and returns ratings for all places added since that date. these results are then emailed to a specified recipient
def send_results(date): 
	conn = psycopg2.connect(
		dbname = os.environ['AWS_DBNAME'],
		host = os.environ['AWS_HOST'], 
		port = os.environ['AWS_PORT'], 
		user = os.environ['AWS_USER'], 
		password = os.environ['AWS_PASSWORD'])
	cur = conn.cursor()	
	cur.execute("SELECT id,name,city FROM glp_items WHERE created_at > date '\'', %s, '\'' AND created_at < date '\'', %s, '\'' + INTERVAL '1 day'" % (date, date) )
	rows = [[row[0], row[1], ''] if row[2] is None else [row[0], row[1], row[2]] for row in cur.fetchall()]
	print len(rows), 'places added on %s' % (date)
	
	distributions = produce_distributions()
	print 'got distributions'

	approved, disapproved, uncertain, couldnt_scrape = [], [], [], []
	for row in rows:
		bsln = baseline(row[0])
		glp_score = produce_glp_score(row[0], distributions)
		combined = combine(bsln, glp_score)
		information = str(row[1]) + ', ' + str(row[2]) + '\nbaseline:\t' + str(bsln) + '\nglp_modifier:\t' + str(glp_score) + '\ncombined:\t' + str(combined) + '\n'
		
		if bsln[2] == 'approve': approved.append(information)
		if 'disapprove' in bsln[2] and 'uncertain' not in bsln[2]: disapproved.append(information)
		if 'uncertain' in bsln[2]: uncertain.append(information)
		if 'scrape' in bsln[2]: couldnt_scrape.append(information)

	results = ['Hi,\nHere are the ratings for the %s places added on %s.' % (len(rows), date.replace('-',' '))]
	results.append('\nAPPROVED: ' + str(len(approved)))
	results.append(approved)
	results.append('\nDISAPPROVED: ' + str(len(disapproved)))
	results.append(disapproved)
	results.append('\nUNCERTAIN: ' + str(len(uncertain)))
	results.append(uncertain)
	results.append('\nCOULDN\'T SCRAPE: ' + str(len(couldnt_scrape)))
	results.append(couldnt_scrape)

	return requests.post(
		"https://api.mailgun.net/v3/%s.mailgun.org/messages" % os.environ['MAILGUN_SANDBOX'],
		auth=("api", os.environ['MAILGUN_API_KEY']),
		data={"from": "Columbus <mailgun@%s.mailgun.org>" % os.environ['MAILGUN_SANDBOX'],
			  "to": ["harrison@greatlittleplace.com"],
			  "subject": "Test Results %s" % (date),
			  "text": results})