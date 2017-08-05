package main

import (
	"testing"
)

const comments = `{"subreddit_id":"t5_2rmov","link_id":"t3_55aife","subreddit":"pokemontrades","created_utc":1475280000,"retrieved_on":1478225196,"stickied":false,"author_flair_text":"3797-8636-2458 || Mary (\u03b1S, Y)","score":1,"controversiality":0,"author":"Emm1096","edited":false,"distinguished":null,"id":"d88zq3w","gilded":0,"author_flair_css_class":"shinycharm","parent_id":"t1_d88zo95","body":"good c: hopefully you're able to get a code!"}
	{"author_flair_css_class":null,"gilded":0,"edited":false,"distinguished":null,"id":"d88zq3x","author":"Orangematz","body":"Also check out [this page](https://www.reddit.com/r/laptops/wiki/index). Lots of good information that may help you.","parent_id":"t1_d88yp3s","retrieved_on":1478225196,"created_utc":1475280000,"stickied":false,"link_id":"t3_555i4a","subreddit":"SuggestALaptop","subreddit_id":"t5_2s4k5","author_flair_text":"Moderator ","controversiality":0,"score":2}
	{"retrieved_on":1478225196,"created_utc":1475280000,"stickied":false,"subreddit_id":"t5_2rxse","link_id":"t3_555c08","subreddit":"reddevils","controversiality":0,"author_flair_text":"Law","score":1,"gilded":0,"author_flair_css_class":"10","author":"Darmian","edited":false,"distinguished":null,"id":"d88zq3y","parent_id":"t3_555c08","body":"Away tickets are sold by the away club mate, not United. "}
	{"stickied":false,"created_utc":1475280000,"retrieved_on":1478225196,"link_id":"t3_55ag0n","subreddit":"AskReddit","subreddit_id":"t5_2qh1i","controversiality":0,"author_flair_text":null,"score":1,"author_flair_css_class":null,"gilded":0,"id":"d88zq3z","edited":false,"distinguished":null,"author":"funnyAlcoholic","body":"Yeah when I was about 12, I went on aol chat rooms with strangers. It was pretty exciting at the time. Learning all the cool new slang like 'lol' and 'a/s/l' and learning how exciting it was to tell someone you were definitely 18 years old and we could talk like adults talk! ","parent_id":"t3_55ag0n"}
	{"body":"Ordered a bottle.  I'm hoping it's decent, but will go into it with low expectations.   \nLooking forward to the AMA","parent_id":"t3_558x75","id":"d88zq40","distinguished":null,"edited":false,"author":"LecheConCarnie","author_flair_css_class":"XXXX","gilded":0,"author_flair_text":"Wiser's_Last_Barrels","controversiality":0,"score":1,"subreddit":"worldwhisky","link_id":"t3_558x75","subreddit_id":"t5_2vadq","stickied":false,"retrieved_on":1478225196,"created_utc":1475280000}`

func TestReadMessages(t *testing.T) {
}
