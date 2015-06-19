require 'bunny'

abort "Usage: #{$0} [routing key] [message] [priority]" if ARGV.size < 3

conn = Bunny.new(:hostname => "192.168.56.101", :username => "mktnav-dev", :password => "C0mplexPwd")
conn.start

ch = conn.create_channel
exchange = ch.topic("mktnav")

routing_key = ARGV[0]
msg = ARGV[1]
priority = ARGV[2].to_i
exchange.publish(msg, :routing_key => routing_key, :priority => priority)
puts " [x] Sent #{routing_key}:#{msg} with priority #{priority}"

conn.close

