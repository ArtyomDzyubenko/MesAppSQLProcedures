create database MesApp;	---�������� �� � ���������� ��������� �������---

use MesApp;

create table node001 (
	begin_event datetime,
	end_event datetime,
	operation_mode int
);

insert into node001
values('2019-12-13 13:00:00.0', '2019-12-14 13:00:00.0', 3);

insert into node001
values('2019-12-14 13:00:00.0', '2019-12-15 13:00:00.0', 3);

insert into node001
values('2019-12-15 13:00:00.0', '2019-12-16 13:00:00.0', 5);

insert into node001
values('2019-12-16 13:00:00.0', '2019-12-17 13:00:00.0', 5);

insert into node001
values('2019-12-17 13:00:00.0', '2019-12-18 13:00:00.0', 1);

insert into node001
values('2019-12-18 13:00:00.0', '2019-12-19 13:00:00.0', 7);

create procedure GetInterval --��������������� ��������� ��� �������� ���������� ��������� � ������
(
	@start_period datetime, 
	@end_period datetime,
	@interval int output
)
as
begin
	select @interval =
			((DATEPART((YEAR), @end_period)) - (DATEPART((YEAR), @start_period))) * 525600 +	--���������� ����� � ���� (���������� ���� � ���� ������ �� �����������)--
			((DATEPART((MONTH), @end_period)) - (DATEPART((MONTH), @start_period))) * 43800 +	--� ������
			((DATEPART((DAY), @end_period)) - (DATEPART((DAY), @start_period))) * 1440 +		--� ���	
			((DATEPART((HOUR), @end_period)) - (DATEPART((HOUR), @start_period))) * 60 +		--� ����
			((DATEPART((MINUTE), @end_period)) - (DATEPART((MINUTE), @start_period)));
end;

--------------------------------------------------------------

create procedure GetNodeAccidentTime	---����� ������� ��-�� ������--
(
	@start_period datetime,
	@end_period datetime,
	@node_name nvarchar(64),
	@downtime int output,
	@downtime_percent decimal output
)
as
begin
	declare @query nvarchar(2048)
	
	set @query =						--��� ������� (����� ������������) ����� ��������, ���������� ������������ ������
	'declare @start_datetime datetime, 
			 @end_datetime datetime, 
			 @interval int, 
			 @downtime int

	set @downtime = 0

	declare downtime_cursor cursor local for											--��� ������� �� ������� � ���������� ���������� ���������� ������
				select begin_event, end_event	
				from ' +  @node_name +													--����� ������������ (�������) ����� �������� �����������
				' where (begin_event >= ''' + convert(varchar, @start_period, 121) +	--������� �������
				''' and end_event <= ''' + convert(varchar, @end_period, 121) + ''') 
				and (operation_mode = 3 or operation_mode = 5)
		
	open downtime_cursor

	fetch next from downtime_cursor
	into @start_datetime, @end_datetime
	
	while @@FETCH_STATUS = 0
		begin
			fetch next from downtime_cursor
			into @start_datetime, @end_datetime

			exec GetInterval @start_datetime, @end_datetime, @interval output			--��� ������ ������ ������� ������������ ����� �������

			set @downtime = @downtime + @interval										--������������ ����� ����� �������
		end

	close downtime_cursor

	select @downtimeOUT = @downtime'

	declare @total_interval int,
			@params nvarchar(500)

	set @total_interval = 0
	set @params = '@downtimeOUT int output'

	exec GetInterval @start_period, @end_period, @total_interval output							--������������ ����� �����							
	exec sp_executesql @query, @params, @downtimeOUT = @downtime output							--��������� ������ � �������� ����� �������

	set @downtime_percent = cast(cast(@downtime as decimal) / @total_interval * 100 as decimal)	--������������ ������� ������� �� ������ �������
end;

create procedure GetNodeStopTime	--����� ������� ��-�� �������� ������������, ��������� ��������� ���������� ����������, �� ����������� ������� �������
(
	@start_period datetime,
	@end_period datetime,
	@node_name nvarchar(64),
	@downtime int output,
	@downtime_percent decimal output
)
as
begin
	declare @query nvarchar(2048)

	set @query = 
	'declare @start_datetime datetime, 
			 @end_datetime datetime, 
			 @interval int, 
			 @downtime int

	set @downtime = 0

	declare stoptime_cursor cursor local for
		select begin_event, end_event
		from ' + @node_name + 
		' where (begin_event >= ''' + convert(varchar, @start_period, 121) + 
		''' and end_event <= ''' + convert(varchar, @end_period, 121) + ''') 
		and  (operation_mode between 1 and 2) or operation_mode = 4 or (operation_mode between 6 and 19)

	open stoptime_cursor

	fetch next from stoptime_cursor
	into @start_datetime, @end_datetime
	
	while @@FETCH_STATUS = 0
		begin
			fetch next from stoptime_cursor
			into @start_datetime, @end_datetime

			exec GetInterval @start_datetime, @end_datetime, @interval output

			set @downtime = @downtime + @interval
		end

	close stoptime_cursor

	select @downtimeOUT = @downtime;'

	declare @total_interval int,
			@params nvarchar(500)

	set @total_interval = 0
	set @params = '@downtimeOUT int output'

	exec GetInterval @start_period, @end_period, @total_interval output
	exec sp_executesql @query, @params, @downtimeOUT = @downtime output

	set @downtime_percent = cast(cast(@downtime as decimal) / @total_interval * 100 as decimal)
end;

create procedure GetNodeUptime --������������ ����� �������� ���������
(
	@start_period datetime,
	@end_period datetime,
	@node_name nvarchar(64),
	@uptime int output,
	@uptime_percent decimal output
)
as
begin
	declare @accident_time int, 
			@stop_time int, 
			@total_interval int,
			@stop_time_percent decimal,
			@accident_time_percent decimal

	set @uptime = 0

	exec GetInterval @start_period, @end_period, @total_interval output	--����� �����
	exec GetNodeAccidentTime @start_period, @end_period, @node_name,@accident_time output, @accident_time_percent output --����� � ������� ������
	exec GetNodeStopTime @start_period, @end_period, @node_name, @stop_time output, @stop_time_percent output	--����� � ������� ���������

	set @uptime = @total_interval - @stop_time - @accident_time	--����� � ������
	set @uptime_percent = cast((100 - @stop_time_percent - @accident_time_percent) as decimal) --������� � ������
end;

declare @start_period datetime, @end_period datetime, @out int, @out_percent decimal, @node_name nvarchar(32) --�������� ������
set @start_period = cast('2019-12-13 13:00:00.0' as datetime)
set @end_period = cast('2019-12-21 13:00:00.0' as datetime)
set @node_name ='node001'

exec GetNodeUptime @start_period, @end_period, @node_name, @out_percent output, @out output	--�������� �������
select @out;
select @out_percent;

--exec GetNodeAccidentTime @start_period, @end_period, @node_name, @out_percent output, @out output
--select @out;
--select @out_percent;

--exec GetNodeStopTime @start_period, @end_period, @node_name, @out_percent output, @out output
--select @out;
--select @out_percent;
