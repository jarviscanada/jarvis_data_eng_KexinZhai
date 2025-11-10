insert into cd.facilities
(facid, name, membercost, guestcost, initialoutlay, monthlymaintenance)
values (9, 'Spa', 20, 30, 100000, 800);


insert into cd.facilities
(facid, name, membercost, guestcost, initialoutlay, monthlymaintenance)
select (select max(facid) from cd.facilities)+1, 'Spa', 20, 30, 100000, 800;


update cd.facilities
set initialoutlay = 10000
where facid = 1;


update cd.facilities facs
set
    membercost = (select membercost * 1.1 from cd.facilities where facid = 0),
    guestcost = (select guestcost * 1.1 from cd.facilities where facid = 0)
where facs.facid = 1;


delete from cd.bookings;


delete from cd.members where memid = 37;


select facid, name, membercost, monthlymaintenance
from cd.facilities
where
    membercost > 0 and
    (membercost < monthlymaintenance/50.0);


select *
from cd.facilities
where name like '%Tennis%';


select *
from cd.facilities
where facid in (1,5);


select memid, surname, firstname, joindate
from cd.members
where joindate >= '2012-09-01';


select surname
from cd.members
union
select name
from cd.facilities;


select bks.starttime
from
    cd.bookings bks
        inner join cd.members mems
                   on mems.memid = bks.memid
where
    mems.firstname='David'
  and mems.surname='Farrell';


select bks.starttime as start, facs.name as name
from
    cd.facilities facs
        inner join cd.bookings bks
                   on facs.facid = bks.facid
where
    facs.name in ('Tennis Court 2','Tennis Court 1') and
    bks.starttime >= '2012-09-21' and
    bks.starttime < '2012-09-22'
order by bks.starttime;


select mems.firstname as memfname, mems.surname as memsname, recs.firstname as recfname, recs.surname as recsname
from
    cd.members mems
        left join cd.members recs
                  on recs.memid = mems.recommendedby
order by memsname, memfname;


select distinct recs.firstname as firstname, recs.surname as surname
from
    cd.members mems
        inner join cd.members recs
                   on recs.memid = mems.recommendedby
order by surname, firstname;


select distinct mems.firstname || ' ' ||  mems.surname as member,
                (select recs.firstname || ' ' || recs.surname as recommender
                 from cd.members recs
                 where recs.memid = mems.recommendedby
                )
from
    cd.members mems
order by member;


select recommendedby, count(*)
from cd.members
where recommendedby is not null
group by recommendedby
order by recommendedby;


select facid, sum(slots) as "Total Slots"
from cd.bookings
group by facid
order by facid;


select facid, sum(slots) as "Total Slots"
from cd.bookings
where
    starttime >= '2012-09-01'
  and starttime < '2012-10-01'
group by facid
order by sum(slots);


select facid, extract(month from starttime) as month, sum(slots) as "Total Slots"
from cd.bookings
where
    starttime >= '2012-01-01'
  and starttime < '2013-01-01'
group by facid, month
order by facid, month;


select count(distinct memid) from cd.bookings


select mems.surname, mems.firstname, mems.memid, min(bks.starttime) as starttime
from cd.bookings bks
         inner join cd.members mems on
    mems.memid = bks.memid
where starttime >= '2012-09-01'
group by mems.surname, mems.firstname, mems.memid
order by mems.memid;


select (select count(*) from cd.members) as count, firstname, surname
from cd.members
order by joindate


select row_number() over(order by joindate), firstname, surname
from cd.members
order by joindate


select facid, total from (
                             select facid, sum(slots) total, rank() over (order by sum(slots) desc) rank
                             from cd.bookings
                             group by facid
                         ) as ranked
where rank = 1


select surname || ', ' || firstname as name from cd.members


select memid, telephone from cd.members where telephone ~ '[()]';


select substr (mems.surname,1,1) as letter, count(*) as count
from cd.members mems
group by letter
order by letter