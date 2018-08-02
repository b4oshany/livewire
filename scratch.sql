select nodes.lw_id, nodes.g from burlap.nodes 
join burlap.lines on st_dwithin(nodes.g,lines.g,.001)
where lines.g  && st_expand((select nodes.g from burlap.nodes where lw_id = 9920),25000)
group by nodes.lw_id
having count(lines.lw_id) = 1




/* get nearest x amount of deadend nodes to a given point*/
select lw_id from (
  select nodes.lw_id from burlap.nodes 
  join burlap.lines on st_dwithin(nodes.g,lines.g,.001)
  order by (select nodes.g from burlap.nodes where lw_id = 9920) <-> nodes.g limit 20000) as foo
group by lw_id
having count(lw_id) = 1