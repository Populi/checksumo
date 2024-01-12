require "rake/clean"
require "rspec/core/rake_task"

CLEAN << "spec/output"
CLEAN << "spec/output.log"
CLEAN << "spec/reports"
CLEAN << "rspec.log"
CLEAN << "build"
CLEAN << "logs/"
CLEAN << "*.log"

task :test do
  RSpec::Core::RakeTask.new(:spec) do |t|
    t.rspec_opts = "--format documentation --format html --out spec/reports/rspec_results.html"
  end

  Rake::Task["spec"].execute
end

task :default => :test
