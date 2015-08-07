import urllib2
import json
import sys 
import collections
import csv 
import MySQLdb

lookup_prefix = "http://itunes.apple.com/lookup?id="
json_file = open("data.json")
data = json.load(json_file)
json_file.close()
db = MySQLdb.connect(host = "localhost", 
                 user="root",
                 passwd="pgd",
                 db="spider")
cur = db.cursor()
for item in data:
    refid = item["refid"]
    url = lookup_prefix + refid
    print "loading from ", url 
    response = urllib2.urlopen(url)
    print "loaded from ", url, type(response)
    remote_data = json.load(response)["results"][0]
    for key in remote_data.keys():
        item[key] = remote_data[key]
    print item
    sql = "insert into appannie_iphone_simple (trackid, rank, kind, features, supportedDevices, advisores, isGameCenterEnabled, genres, price, currency, genreIds, sellerName, bundleId, trackName, primaryGenreName, primaryGenreId, wrapperType, formattedPrice, trackCensoredName, trackViewUrl, contentAdvisoryRating, languageCodesISO2A, fileSizeBytes, sellerUrl, averageUserRatingForCurrentVersion, userRatingCountForCurrentVersion, trackContentRating, averageUserRating, userRatingCount) values (";
    sql += ("" + str((item[u"trackId"])) + ", ")
    sql += (""+ str(item[u"rank"]) + ", ")
    sql += ("' " + item["currency"] + "', ")
    sql += ("' " + str(item["genreIds"]) + "', ")
    sql += ("' " + item["sellerName"] + "', ")
    sql += ("' " + item["bundleId"] + "', ")
    sql += ("' " + item["trackName"] + "', ")
    sql += ("' " + item["primaryGenreName"] + "', ")
    sql += (" " + str(item["primaryGenreId"]) + ", ")
    sql += ("' " + str(item["wrapperType"]) + "', ")
    sql += ("' " + item["formattedPrice"] + "', ")
    sql += ("' " + item["trackCensoredName"] + "', ")
    sql += ("' " + item["trackViewUrl"] + "', ")
    sql += ("' " + item["contentAdvisoryRating"] + "', ")
    sql += ("' " + str(item["languageCodesISO2A"]) + "', ")
    sql += ("' " + item["fileSizeBytes"] + "', ")
    sql += ("' " + item["sellerUrl"] + "', ")
    sql += ("' " + str(item["averageUserRatingForCurrentVersion"]) + "', ")
    sql += ("' " + str(item["userRatingCountForCurrentVersion"]) + "', ")
    sql += ("' " + item["trackContentRating"] + "', ")
    sql += ("' " + str(item["averageUserRating"]) + "', ")
    sql += (" " + str(item["userRatingCount"]) + ")") 
    print type(sql), sql
    cur.execute(sql)
    break
db.close()
