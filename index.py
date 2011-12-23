#!/usr/bin/env python

# Copyright (c) 2011 Google, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Automatically decompresses your Stack Exchange data dump and builds
sqlite3 databases from the XML files it contains.

Run this script in the same directory as your downloaded data dump from
Stack Exchange.
"""

import glob
import MySQLdb
import optparse
import os
import string
import sys
import time
import xml.etree.cElementTree as et
from collections import defaultdict

WORDLETTERS = frozenset(string.ascii_lowercase + string.digits)

z7 = '7za'  # You can overwrite this if you have a different 7zip binary.

def delete_db(name):
  conn = MySQLdb.connect(db='stackoverflow', use_unicode=True, charset='utf8') 
  c = conn.cursor()
  c.execute('delete from %s_comments;' % name)
  c.execute('delete from %s_posts;' % name)
  c.execute('delete from %s_users;' % name)
  c.execute('drop table %s_comments;' % name)
  c.execute('drop table %s_posts;' % name)
  c.execute('drop table %s_users;' % name)
  conn.commit()
  c.close()
  conn.close()

def setup_db(name):
  conn = MySQLdb.connect(db='stackoverflow', use_unicode=True, charset='utf8') 
  c = conn.cursor()
  prefix = name
  print name
  c.execute('''create table %s_users (
    id integer(8) not null primary key,
    reputation integer(8),
    creationdate datetime,
    displayname varchar(255) character set utf8,
    emailhash char(32),
    lastaccessdate datetime,
    location varchar(255) character set utf8,
    aboutme text character set utf8,
    views integer(8),
    upvotes integer(8),
    downvotes integer(232),
    age integer(3),
    websiteurl varchar(255) character set utf8
    )''' % prefix)
  c.execute('''create table %s_posts (
    id integer(8) not null primary key,
    parentid integer(8),
    posttypeid integer(8),
    acceptedanswerid integer(8),
    creationdate datetime,
    closeddate datetime,
    communityowneddate datetime,
    lastactivitydate datetime,
    owneruserid integer(8),
    lasteditoruserid integer(8),
    lasteditordisplayname varchar(255) character set utf8,
    lasteditdate datetime,
    score integer(8),
    viewcount integer(8) default 0,
    title varchar(255) character set utf8,
    body text character set utf8,
    tags varchar(255) character set utf8,
    answercount integer(8),
    commentcount integer(8),
    favoritecount integer(8)
      )''' % prefix)
  c.execute('''create fulltext index %s_posts_title on %s_posts (title)''' % (prefix, prefix))
  c.execute('''create fulltext index %s_posts_body on %s_posts (body)''' % (prefix, prefix))
  c.execute('''create fulltext index %s_posts_tags on %s_posts (tags)''' % (prefix, prefix))
  c.execute('''create index %s_posts_parentid on %s_posts (parentid)''' % (prefix, prefix))
  c.execute('''create index %s_posts_owneruserid on %s_posts (owneruserid)''' % (prefix, prefix))
  c.execute('''create table %s_comments (
    id integer(8) not null primary key,
    postid integer(8) not null,
    score integer(8),
    text text character set utf8,
    creationdate datetime,
    userid integer(8)
  )''' % prefix)
  c.execute('''create index %s_comments_postid on %s_comments (postid)''' % (prefix, prefix))
  c.execute('''create index %s_comments_userid on %s_comments (userid)''' % (prefix, prefix))
  c.execute('''create fulltext index %s_comments_text on %s_comments (text)''' % (prefix, prefix))
  conn.commit()
  return conn, c

def create_users(name, cursor, users):
  print "  Creating users" 
  table = "%s_users" % name
  for x in users:
    keyvalues = [(k, x.get(k)) for k in x.keys()]
    create(cursor, table, keyvalues)

def create(cursor, table, keyvalues):
  keys = [k for k, v in keyvalues]
  values = [v for k, v in keyvalues]
  query = 'insert into %s (%s) values (%s);' % (table, ','.join(keys), ','.join(['%s'] * len(values)))
  cursor.execute(query, values)

def create_posts(name, cursor, posts):
  print "  Creating posts"
  table = "%s_posts" % name
  for x in posts:
    keyvalues = [(k, x.get(k)) for k in x.keys()]
    create(cursor, table, keyvalues)

def create_comments(name, cursor, comments):
  print "  Creating comments"
  table = "%s_comments" % name
  for x in comments:
    keyvalues = [(k, x.get(k)) for k in x.keys()]
    create(cursor, table, keyvalues)

def stripout(word):
  return ''.join(x for x in word if x in WORDLETTERS)

def get_words(text):
  if not text:
    return []
  words = text.lower().strip().split()
  words = [stripout(w) for w in words]
  words = set(w for w in words if w)
  return sorted(words)

def get_tags(post):
  tags = post.get('Tags')
  if not tags:
    return []
  tags = tags[1:-1].split('><')
  return sorted(set(t for t in tags if t))

def check_dir(directory):
  if os.path.exists(os.path.join(directory, 'posts.xml')) and \
     os.path.exists(os.path.join(directory, 'users.xml')) and \
     os.path.exists(os.path.join(directory, 'comments.xml')):
    return True
  return False

def db_exists(dbname):
  conn = MySQLdb.connect(db='stackoverflow', use_unicode=True, charset='utf8') 
  c = conn.cursor()
  c.execute('show tables;')
  row = c.fetchone()
  while row is not None:
    table_name = row[0]
    if unicode(dbname) == table_name.split('_')[0]:
      return True
    row = c.fetchone() 
  return False

def parse(f):
  for line in open(f):
    l = line.strip()
    if not l.startswith('<row'): continue
    yield et.fromstring(l)

def main(options):
  start_time = time.time()
  if options.unzip:
    for z in glob.glob('*.7z') + glob.glob('*.7z.001'):
      os.system('echo s | ' + z7 + ' x ' + z)
  
  posts = glob.glob('**/posts.xml')
  dirs = set(x[:-10] for x in posts)
  for directory in dirs:
    dir_start_time = time.time()
    check_dir(directory)
    dbname = ''.join(directory.lower().split(' ')[1:]) 
    dbname = dbname.replace('-', '')
    print "Processing [%s] => %s" % (directory, dbname)
    do_db = True 
    if db_exists(dbname):
      if options.delete:
        print 'Deleting previous entries'
        delete_db(dbname)
      else:
        print '  Database exists... skipping.'
        do_db = False
    posts_file, users_file, comments_file = [os.path.join(directory, x) for x in ('posts.xml', 'users.xml', 'comments.xml')]
    users = parse(users_file)
    if options.mysql and do_db:
      users = parse(users_file)
      conn, cursor = setup_db(dbname)
      create_users(dbname, cursor, users)
      conn.commit()
    posts = parse(posts_file)
    if options.mysql and do_db:
      posts = parse(posts_file)
      create_posts(dbname, cursor, posts)
      conn.commit()
    comments = parse(comments_file)
    if options.mysql and do_db:
      comments = parse(comments_file)
      create_comments(dbname, cursor, comments)
      conn.commit()
      cursor.close()
    dir_end_time = time.time()
    print '  Time: %.f seconds' % (dir_end_time - dir_start_time)
  end_time = time.time()
  print "Total time: %.f seconds" % (end_time - start_time)

if __name__ == '__main__':
  option_parser = optparse.OptionParser()
  option_parser.add_option('-z', '--unzip', default=False, action='store_true',
                           help='Unzip the .7z files')
  option_parser.add_option('-d', '--delete', default=False, action='store_true',
                           help='Delete any existing tables of the same names')
  option_parser.add_option('-M', '--no-mysql', default=True, action='store_false',
                           dest='mysql', help='Don\'t write to MySQL')

  options, args = option_parser.parse_args()
  main(options)
