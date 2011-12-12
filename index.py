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
import os
import sys
import sqlite3
import time
import xml.etree.cElementTree as et

z7 = '7za'  # You can overwrite this if you have a different 7zip binary.

start_time = time.time()

def setup_db(name):
  conn = sqlite3.connect(name)
  c = conn.cursor()
  c.execute('''create table users (
    id integer(8) not null primary key,
    reputation integer(8),
    creationdate datetime,
    displayname varchar(255),
    emailhash char(32),
    lastaccessdate datetime,
    location varchar(255),
    aboutme text,
    views integer(8),
    upvotes integer(8),
    downvotes integer(232),
    age integer(3),
    websiteurl varchar(255)
    )''')
  c.execute('''create table posts (
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
    lasteditordisplayname varchar(255),
    lasteditdate datetime,
    score integer(8),
    viewcount integer(8),
    title varchar(255),
    body text,
    tags varchar(255),
    answercount integer(8),
    commentcount integer(8),
    favoritecount integer(8)
      )''')
  c.execute('''create table comments (
    id integer(8) not null primary key,
    postid integer(8) not null,
    score integer(8),
    text text,
    creationdate datetime,
    userid integer(8)
  )''')
  conn.commit()
  return conn, c

def create_users(cursor, users):
  print "  Creating %d users" % len(users)
  sys.stdout.write('  ')
  i = 0
  for x in users:
    if i & 0xfff == 0:
      sys.stdout.write('.')
      sys.stdout.flush()
    i += 1
    keyvalues = [(k, x.get(k)) for k in x.keys()]
    create(cursor, 'users', keyvalues)
  sys.stdout.write('\n')
  sys.stdout.flush()

def create(cursor, table, keyvalues):
  keys = [k for k, v in keyvalues]
  values = [v for k, v in keyvalues]
  query = 'insert into %s (%s) values (%s);' % (table, ','.join(keys), ','.join(['?'] * len(values)))
  cursor.execute(query, values)

def create_posts(cursor, posts):
  print "  Creating %d posts" % len(posts)
  sys.stdout.write('  ')
  i = 0
  for x in posts:
    if i & 0xfff == 0:
      sys.stdout.write('.')
      sys.stdout.flush()
    i += 1
    keyvalues = [(k, x.get(k)) for k in x.keys()]
    create(cursor, 'posts', keyvalues)
  sys.stdout.write('\n')
  sys.stdout.flush()

def create_comments(cursor, comments):
  print "  Creating %d comments" % len(comments)
  sys.stdout.write('  ')
  i = 0
  for x in comments:
    if i & 0xfff == 0:
      sys.stdout.write('.')
      sys.stdout.flush()
    i += 1
    keyvalues = [(k, x.get(k)) for k in x.keys()]
    create(cursor, 'comments', keyvalues)
  sys.stdout.write('\n')
  sys.stdout.flush()

def check_dir(directory):
  if os.path.exists(os.path.join(directory, 'posts.xml')) and \
     os.path.exists(os.path.join(directory, 'users.xml')) and \
     os.path.exists(os.path.join(directory, 'comments.xml')):
    return True
  return False


for z in glob.glob('*.7z') + glob.glob('*.7z.001'):
  os.system('echo s | ' + z7 + ' x ' + z)

posts = glob.glob('**/posts.xml')
dirs = set(x[:-10] for x in posts)
for directory in dirs:
  dir_start_time = time.time()
  check_dir(directory)
  dbname = ''.join(directory.lower().split(' ')[1:]) + '.sqlite3'
  print "Processing [%s] => %s" % (directory, dbname)
  if os.path.exists(dbname): 
    print '  Database exists... skipping.'
    continue
  posts_file, users_file, comments_file = [os.path.join(directory, x) for x in ('posts.xml', 'users.xml', 'comments.xml')]
  users = et.parse(users_file).getroot()
  conn, cursor = setup_db(dbname)
  create_users(cursor, users)
  del users
  conn.commit()
  posts = et.parse(posts_file).getroot()
  create_posts(cursor, posts)
  del posts
  conn.commit()
  comments = et.parse(comments_file).getroot()
  create_comments(cursor, comments)
  del comments
  conn.commit()
  cursor.close()
  dir_end_time = time.time()
  print '  Time: %.f seconds' % (dir_end_time - dir_start_time)
end_time = time.time()
print "Total time: %.f seconds" % (end_time - start_time)