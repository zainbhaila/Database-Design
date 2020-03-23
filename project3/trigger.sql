create or replace function medc_func()
  returns trigger as
  $$
  begin
    drop table MedalCounts;
    create table if not exists MedalCounts as
      select p.player_id, name, count(*) as num_medals
      from results r, players p
      where r.player_id = p.player_id
      group by p.player_id, name;
    return null;
  end;
  $$
  LANGUAGE plpgsql VOLATILE;

DROP TRIGGER IF EXISTS medc ON results;
create trigger medc
  after insert or delete
  on results
  for each row
  execute procedure medc_func();
