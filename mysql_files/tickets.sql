SELECT event_name, sum(num_tickets) as total_tickets FROM ticket_table GROUP BY event_id  ORDER BY total_tickets DESC LIMIT 3