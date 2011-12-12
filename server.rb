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


# This is the main server that serves the Stack Exchange data dumps.

require 'rubygems'

# Load vendor/*
Dir["#{File.dirname(__FILE__)}/vendor/**"].each { |dir|
  $LOAD_PATH.unshift dir + '/lib'
}

require 'sinatra'
require 'sequel'

# Load databases
dbs = {}
Dir["#{File.dirname(__FILE__)}/*.sqlite3"].each { |dbfile|
  dbfile = dbfile.sub(/\.\//, '')
  puts dbfile
  db = Sequel.connect("sqlite://#{dbfile}")
  dbname = dbfile.sub(/\.sqlite3/, '')
  dbs[dbname] = db
}


layout 'layout'
set :public_folder, File.dirname(__FILE__) + '/static'

helpers do
  def find_user_by_id(x)
    users = @db[:users].where(:id => x).first(1)
    if users.empty?
      nil
    else
      users[0]
    end
  end
  def userlink(x)
    user = find_user_by_id(x)
    if user.nil?
      nil
    else
      "<a href=\"/#{@site}/user/#{x}\">#{user[:displayname]}</a>"
    end
  end
  def postlink(x)
    "/#{@site}/post/#{x}"
  end
end

# Cache *ALL* the pages!
before do
  cache_control :public, :max_age => 3600
end

before '/:site/*' do
  @title = ''
  @site = params[:site]
  @action = "/#{@site}/search"
  @db = dbs[params[:site]] if dbs.include?(params[:site])
end

get '/' do
  @site = nil
  @action = '/stackoverflow/search'
  @title = 'Stack Exchange Mirror'
  @names = dbs.map { |name, _| name }.sort
  erb :index
end

get '/:site/?' do
  erb :site
end

get '/:site/search/?' do
  @query = params[:q]
  @posts = @db[:posts].filter(:body.like("%#{@query}%")).order(:score).reverse.first(25)
  erb :search
end

get '/:site/user/:id' do
  @user_keys = [:displayname, :reputation, :age, :location, :upvotes, :downvotes, :views, :creationdate, :lastaccessdate, :aboutme, :websiteurl]
  @user = find_user_by_id(params[:id])
  halt(404) if @user.nil?
  erb :user
end

get '/:site/post/:id' do
  post = @db[:posts].where(:id => params[:id]).first(1)
  halt(404) if post.empty?
  @post = post[0]
  @answers = @db[:posts].where(:parentid => params[:id]).all
  @accepted = @post[:acceptedanswerid] or -1
  @comments = @db[:comments].where(:postid => params[:id]).all
  @comments = @comments.sort_by { |comment| -(comment[:score] or 0) }
  erb :post
end
