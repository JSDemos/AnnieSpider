import urllib2
import json
import sys 
import collections
import csv 
import MySQLdb

LOOKUP_PATTERN = "http://itunes.apple.com/%s/lookup?id=%s"
COUNTRY_LIST = ["", "cn"]
def main():
    db = MySQLdb.connect(host = "localhost", 
                 user="root",
                 passwd="pgd",
                 db="spider")
    db.set_character_set("utf8")
    cur = db.cursor()
    insert_all(cur)
    db.commit()
    db.close()

def get_remote_data(refid, item):
    for locale in COUNTRY_LIST:
        url = LOOKUP_PATTERN % (locale, refid)
        logv("spider", "url : " + url)
        response = urllib2.urlopen(url)
        remote_data = json.load(response)["results"]
        if len(remote_data) > 0:
           return remote_data[0] 
    loge("spider", "sadlly, noinfo at" + refid + item["title"])

def insert_all(cur):
    json_file = open("data.json")
    data = json.load(json_file)
    json_file.close()
    cur.execute("delete from table_demo")
    for item in data:
        refid = item["refid"]
        remote_data = get_remote_data(refid, item) 
        if remote_data is None:
            continue
        for key in remote_data.keys():
            item[key] = remote_data[key]
        insert_into(cur,item)

def create_table(cursor):
    cursor.execute(
    """
    create table table_demo(
    trackId int,
    trackName varchar(255),
    sellerName varchar(255),
    kind varchar(255),
    type int, 
    languageCodesISO2A varchar(255),
    userRatingCount int, 
    averageUserRating varchar(255)
    )
    """
    )

def insert_into(cursor, item):
    logv("spider", "item type and " + str(type(item)) +  str(item))
    sql = """
    insert into table_demo(
    trackId, 
    trackName, 
    sellerName,
    kind,
    type,
    languageCodesISO2A, 
    userRatingCount,
    averageUserRating
    )
    values(
     %s,
     %s,
     %s,
     %s,
     %s,
     %s,
     %s,
     %s
    )
    """

    args = (
    item["trackId"],
    item["trackName"],
    item["sellerName"],
    item["kind"],
    item["type"],
    " ".join(item["languageCodesISO2A"]),
    "None" if not item.has_key('userRatingCount') else item["userRatingCount"],
    -1 if not item.has_key('averageUserRating') else item["averageUserRating"] )

    logv("spider", sql)
    cursor.execute(sql, args)

def format(arg):
    if type(arg) == unicode:
        return arg.replace("'", " ")
    else:
        return arg

LEVEL_VERBOSE = 1
LEVEL_WARN = 2
LEVEL_ERROR = 3 
log_level = LEVEL_WARN

def level_str(level):
    if level == LEVEL_VERBOSE:
        return "verbose"
    elif level == LEVEL_WARN:
        return "warn"
    elif level == LEVEL_ERROR:
        return "error"

def log(level, tag, msg):
    if level >= log_level:
        print "[%s]\t%s\t%s" % (level_str(level), tag, msg)

def logv(tag, msg):
    log(LEVEL_VERBOSE, tag, msg)  

def logw(tag, msg):
    log(LEVEL_WARN, tag, msg)  

def loge(tag, msg):
    log(LEVEL_ERROR, tag, msg)  

if __name__ == "__main__":
    main()
