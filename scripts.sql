/* Formatted on 13.10.2015 14:37:29 (QP5 v5.160) */
select pers_acc_id,name_client,adr, vhod_ost,vhod_ost_check, debet1, debet2, debet3, debet4
from (
  select pers_acc_id,name_client, grouping(pers_acc_id) pers_acc_id1, grouping(name_client) name_client2
    , sum(vhod_ost) vhod_ost, sum(nvl(debet1,0)+nvl(debet2,0)+nvl(debet3,0)+nvl(debet4,0)) vhod_ost_check
    , sum(nvl(debet1,0)) debet1, sum(nvl(debet2,0)) debet2, sum(nvl(debet3,0)) debet3, sum(nvl(debet4,0)) debet4,
    adr
  from (
    select pers_acc_id, vhod_ost, denis_pack.LAST_CLIENT_NAME(pers_acc_id) name_client,
    adr
        ,substr(debet, 1, instr(debet,'|',1,1)-1) debet1
        ,substr(debet, instr(debet,'|',1,1)+1 ,instr(debet,'|',1,2)  - instr(debet,'|',1,1)-1) debet2
        ,substr(debet, instr(debet,'|',1,2)+1 ,instr(debet,'|',1,3)  - instr(debet,'|',1,2)-1) debet3
        ,substr(debet, instr(debet,'|',1,3)+1 ,instr(debet,'|',1,4)  - instr(debet,'|',1,3)-1) debet4 
    from (
        select pers_acc_id, vhod_ost,debet.getdebet2(opl,nvl(debet1,0),nvl(debet2,0),nvl(debet3,0),nvl(debet4,0)) debet,adr
        from (
            select  pers_acc_id, sum((select vhod_ost from dual where num = 1)) vhod_ost
                ,sum((select ish_ostatok - vhod_ost + oplata from dual where num = 5))+ sum((select vhod_ost from dual where num = 5)) debet4
                ,sum((select ish_ostatok - vhod_ost + oplata from dual where num = 4)) debet3
                ,sum((select ish_ostatok - vhod_ost + oplata from dual where num = 3)) debet2
                ,sum((select ish_ostatok - vhod_ost + oplata from dual where num = 2)) debet1
                ,sum((select  oplata from dual where num in (2,3,4,5))) opl,adr 
            from (
                select (row_number() over(partition by pers_acc_id order by assign_date desc)) num,pers_acc_id, assign_date, vhod_ost
                ,ish_ostatok, nach*-1,decode(sign(oplata-nach),-1,nach+(oplata-nach)*-1,nach) nach,decode(sign(oplata-nach),-1,0,oplata-nach) oplata  
                ,rasch_show_function.adres_string(addr_id) adr
                from (
                    select /*+ordered index(cd CLIENT_DET_PK) index(b BILL_TELECOM_ASSDATA_I) indeх(ca CLIENT_ASS_PA_SD_ED_I)*/
                distinct b.pers_acc_id, b.summa*-1 vhod_ost
                        ,b.oplata
                        ,b.ish_ostatok*-1 ish_ostatok
                        ,b.assign_date
                        ,nvl( (select sum(summa) nach from nachisl nach, service ser,service_type st 
                                    where nach.bill_id = b.id
                                    and ser.id = nach.serv_id
                                    and st.id = ser.sertype_id
                                    and ser.sertype_id in (61,18)
                                ) ,0) nach,
                                ca.addr_id
                      
                    from   (select t.id from telecom t where 1=work_telecom.telec_in_telec(:tel,t.id)) ttt
                        ,bill b,client_assignment ca,client_detail cd, class cl
                    where b.telecom_id = ttt.id
                        and b.assign_date between add_months(to_date(:dt,'dd.mm.rrrr'),-4) and to_date(:dt,'dd.mm.rrrr')
                         and((:p1 is null and :p2 is null)or( b.pers_acc_id between :p1 and :p2))
                        and ca.pers_acc_id = b.pers_acc_id
                        and ca.start_date is not null
                        

                and ( (ca.end_date is not null and add_months(to_date(:dt,'dd.mm.rrrr'),1)-1 between ca.start_date and add_months(trunc(ca.end_date),1)-1 and ca.start_date=(select max(ca.start_date) from client_assignment where id=ca.id))
                              or (ca.end_date is null and trunc(ca.start_date/*,'mm'*/)<=to_date(:dt,'dd.mm.rrrr')) ) --12.11.2012 вывод по двум категориям

                        and cd.id = ca.clidet_id
                        and cl.id = cd.class_id
                        and (cl.kva_pred=:kat or :kat = -1)
                     and (ca.rayon_id_service=:r or :r=-1)
                    )
             )inf group by pers_acc_id,adr
        ) where vhod_ost > 0
    ) order by pers_acc_id
 ) group by rollup((pers_acc_id,name_client,adr))
) where (pers_acc_id1=0 and name_client2=0) or (pers_acc_id1=1 and name_client2=1) -- убираем не нужные под итоги


