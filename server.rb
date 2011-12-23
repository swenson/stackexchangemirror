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

require 'date'
require 'rubygems'

# Load vendor/*
Dir["#{File.dirname(__FILE__)}/vendor/**"].each { |dir|
  $LOAD_PATH.unshift dir + '/lib'
}

require 'nokogiri'
require 'sinatra'
require 'sequel'

# Load tables

module DBHOLDER
  @@db = nil

  def self.set(x)
    @@db = x
  end

  def self.get
    @@db
  end
end

require 'logger'

DBHOLDER::set Sequel.connect("mysql://localhost/stackoverflow", :user => 'swenson', :socket => '/var/run/mysqld/mysqld.sock', :loggers => [Logger.new($stdout)])

layout 'layout'
set :public_folder, File.dirname(__FILE__) + '/static'

dbs = DBHOLDER::get().tables.map { |x| x.to_s }.select { |x| x.end_with? 'posts' }.map { |x| x.sub(/_posts/, '') }

helpers do  
  def get_random_article_of_the_day(site)
    srand(Date.today.hash)
    count = find_posts(site).where(:score > 25).count()
    find_posts(site).where(:score > 25).limit(1, rand(count)).first(1)[0]
  end
  
  def make_snippet(post)
    doc = Nokogiri::HTML(post[:body])
    doc.xpath("//text()").remove.text[0,100]
  end
  
  def get_tag_cloud(post)
     tags_text = post[:tags]
     tags = tags_text[1,tags_text.size - 2].split('><')
     tags.sort.uniq.map { |tag| taglink(tag) }.join(' ')
  end
  
  def get_site_tag_cloud(site)
    tags_text = find_posts(site).first(25).map { |post| post[:tags] }.join
    tags = tags_text[1,tags_text.size - 2].split('><')
    tags.sort.uniq.map { |tag| taglink(tag) }.join(' ')
  end
  
  def find_user_by_id(x)
    users = find_users(@site).where(:id => x).first(1)
    if users.empty?
      nil
    else
      users[0]
    end
  end
  
  def taglink(x)
    "<a href=\"/#{@site}/tag/#{x}\">#{x}</a>"
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
  
  def find_posts(site)
    @db["#{site}_posts".to_sym]
  end
  
  def find_users(site)
    @db["#{site}_users".to_sym]
  end
  
  def find_comments(site)
    @db["#{site}_comments".to_sym]
  end
end

# Cache *ALL* the pages!
before do
  cache_control :public, :max_age => 3600
end

before '/:site/?*' do
  @title = ''
  @site = params[:site]
  @action = "/#{@site}/search"
  @db = DBHOLDER::get
end

get '/favicon.ico' do
  halt(404)
end

get '/' do
  @site = nil
  @action = '/stackoverflow/search'
  @title = 'Stack Exchange Mirror'
  @names = dbs.map { |name, _| name }.sort
  @tags = ''
  erb :index
end

get '/:site/?' do
  @tags = get_site_tag_cloud(@site)
  @random = get_random_article_of_the_day(@site)
  erb :site
end

get '/:site/search/?' do
  @query = params[:q]
  @posts = find_posts(@site).filter(:body.like("%#{@query}%")).order(:score).reverse.first(25)
  @tags = get_site_tag_cloud(@site)
  erb :search
end

get '/:site/tag/:tag' do
  @tag = params[:tag]
  @query = "tag:#{@tag}"
  @tags = get_site_tag_cloud(@site)
  @posts = find_posts(@site).filter(:tags.like("%#{@tag}%")).order(:score).reverse.first(25)
  erb :search
end

get '/:site/user/:id' do
  @user_keys = [:displayname, :reputation, :age, :location, :upvotes, :downvotes, :views, :creationdate, :lastaccessdate, :aboutme, :websiteurl]
  @user = find_user_by_id(params[:id])
  @tags = get_site_tag_cloud(@site)
  halt(404) if @user.nil?
  erb :user
end

get '/:site/post/:id' do
  post = find_posts(@site).where(:id => params[:id]).first(1)
  halt(404) if post.empty?
  @post = post[0]
  @tags = get_tag_cloud(@post)
  @answers = find_posts(@site).where(:parentid => params[:id]).all
  @accepted = @post[:acceptedanswerid] or -1
  @comments = find_comments(@site).where(:postid => params[:id]).all
  @comments = @comments.sort_by { |comment| -(comment[:score] or 0) }
  erb :post
end
