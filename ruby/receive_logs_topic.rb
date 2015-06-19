require "bunny"

abort "Usage: #{$0} [queue name] [binding key]" if ARGV.size < 2

conn = Bunny.new(:hostname => "192.168.56.101", :username => "mktnav-dev", :password => "C0mplexPwd")
conn.start

queue_name = ARGV[0]
binding_key = ARGV[1]
ch = conn.create_channel
ch.prefetch(1)
exchange = ch.topic("mktnav")
queue = ch.queue(queue_name, :auto_delete => true, :arguments => {"x-max-priority" => 1})
queue.bind(exchange, :routing_key => binding_key)

puts " [*] Waiting for logs. To exit press CTRL+c"
begin
  queue.subscribe(:block => true, :ack => true) do |delivery_info, properties, body|
    ch.acknowledge(delivery_info.delivery_tag, false)
    delay = body.to_i
    print "Waiting for #{delay} seconds..."
    sleep(delay)
    puts "DONE"
  end
rescue Interrupt => _
  ch.close
  conn.close
end

