require "rake/clean"
require "rspec/core/rake_task"

LOGFILES = FileList.glob("logs/*")
LOGFILES << FileList.glob("*.log")
LOGFILES << FileList.glob("*.log.age")
LOGFILES << FileList.glob("spec/*.log")
LOGFILES << FileList.glob("spec/*.log.age")
LOGFILES.flatten!

CLEAN << "spec/output"
CLEAN << "spec/reports"
CLEAN << "build"
LOGFILES.each { |f| CLEAN << f }

task :test do
  RSpec::Core::RakeTask.new(:spec) do |t|
    t.rspec_opts = "--format documentation --format html --out spec/reports/rspec_results.html"
  end

  Rake::Task["spec"].execute
end

task default: :test
