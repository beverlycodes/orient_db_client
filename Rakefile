require "bundler/gem_tasks"
require 'rake/testtask'

Rake::TestTask.new do |t|
	t.name = 'test'
  t.libs.push "lib"
  t.test_files = FileList['test/**/*_test.rb']
  t.verbose = true
end

Rake::TestTask.new do |t|
	t.name = 'test:unit'
  t.libs.push "lib"
  t.test_files = FileList['test/unit/**/*_test.rb']
  t.verbose = true
end

Rake::TestTask.new do |t|
	t.name = 'test:integration'
  t.libs.push "lib"
  t.test_files = FileList['test/integration/**/*_test.rb']
  t.verbose = true
end