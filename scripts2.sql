with clnt as (/*Выбираем список клиентов удовлетворяющих заданым ограничениям отчета*/
              select clnt_clnt_id,sum(pays)pays from (
  select ch.clnt_clnt_id, sum(nvl(p.amount_$,0)) pays
       from bis.client_histories ch
              join (select brgr_id, brnc_id
                   from bis.branches_groups
                   join bis.branches on brgr_brgr_id = brgr_id
                   join table(bis.ktk$get_user_brnc_list(:login)) brnc on brnc.column_value = branches.brnc_id                       
                   where brgr_id = decode(:brgr_id,0,brgr_id,:brgr_id)
                         and brnc_id = decode(:brnc_id,0,brnc_id,:brnc_id)) bg --фильтруем по заданым Области (brances_groups) и району(branches)
                   on bg.brnc_id = ch.brnc_brnc_id
              left join bis.payments p
                   on p.clnt_clnt_id = ch.clnt_clnt_id
                   and p.pay_date between :s_date and :e_date
                   and nvl(p.bill_bill_id,9999e11) > to_char(:mnth_end,'yymm')||'99999999999'
                   and p.del_date is null
              left join bis.adjustments a
                   on a.clnt_clnt_id = ch.clnt_clnt_id
                   and a.adj_date between :s_date and :e_date
                   and nvl(a.bill_bill_id,9999e11) > to_char(:mnth_end,'yymm')||'99999999999'
                   and a.del_date is null
                   and a.ajtp_ajtp_id in (2,5)
              where ch.blgr_blgr_id in (1,2)
              
                    and ch.start_date <= :mnth_end --берем актуальные на момент запуска данные
                    and ch.end_date >:mnth_end                
                    and ch.hist_jrtp_id = decode(:jrtp_id,0,ch.hist_jrtp_id,:jrtp_id) --юр.статус
                    and ch.ctyp_ctyp_id = decode(:ctyp_id,0,ch.ctyp_ctyp_id,:ctyp_id) --категория клиента
                    and ch.ccat_ccat_id = decode(:ccat_id,0,ch.ccat_ccat_id,:ccat_id)
                    and ch.clcl_clcl_id = decode(:clcl_id,0,ch.clcl_clcl_id,:clcl_id) --модель расчетов
                    and ch.clis_clis_id <>4                        
             group by ch.clnt_clnt_id
 union all
        select ch.clnt_clnt_id, sum(nvl(a.amount_$,0)) pays
             from bis.client_histories ch
              join (select brgr_id, brnc_id
                   from bis.branches_groups
                   join bis.branches on brgr_brgr_id = brgr_id
                   join table(bis.ktk$get_user_brnc_list(:login)) brnc on brnc.column_value = branches.brnc_id                       
                   where brgr_id = decode(:brgr_id,0,brgr_id,:brgr_id)
                         and brnc_id = decode(:brnc_id,0,brnc_id,:brnc_id)) bg --фильтруем по заданым Области (brances_groups) и району(branches)
                   on bg.brnc_id = ch.brnc_brnc_id
              left join bis.adjustments a
                   on a.clnt_clnt_id = ch.clnt_clnt_id
                   and a.adj_date between :s_date and :e_date
                   and nvl(a.bill_bill_id,9999e11) > to_char(:mnth_end,'yymm')||'99999999999'
                   and a.del_date is null
                   and a.ajtp_ajtp_id in (2,5)
              where ch.blgr_blgr_id in (1,2)
               
                    and ch.start_date <= :mnth_end --берем актуальные на момент запуска данные
                    and ch.end_date >:mnth_end                
                    and ch.hist_jrtp_id = decode(:jrtp_id,0,ch.hist_jrtp_id,:jrtp_id) --юр.статус
                    and ch.ctyp_ctyp_id = decode(:ctyp_id,0,ch.ctyp_ctyp_id,:ctyp_id) --категория клиента
                    and ch.ccat_ccat_id = decode(:ccat_id,0,ch.ccat_ccat_id,:ccat_id)
                    and ch.clcl_clcl_id = decode(:clcl_id,0,ch.clcl_clcl_id,:clcl_id) --модель расчетов
                    and ch.clis_clis_id <>4                        
              group by ch.clnt_clnt_id
          )   group by clnt_clnt_id 
            )
, debs as (/*ищем дебиторов по результату последнего биллинга*/
        select b.clnt_clnt_id
               , min(new_balance_$) keep(dense_rank first order by bill_date desc) in_debt --исходящий баланс последнего биллинга
               , max(c.pays) pays --платежи, не учтенные в биллинге
               , max(bill_date) d_bill_date --дата биллинга
        from bis.bills b
        join clnt c on c.clnt_clnt_id = b.clnt_clnt_id
        where nvl(b.bltp_bltp_id,0) = 0 
              and b.bill_date <= :mnth_end --ищем те где дата биллинга меньше либо равна посл. сек заданного месяца
        group by b.clnt_clnt_id
          
      having min(new_balance_$) keep(dense_rank first order by bill_date desc) > 0 --только те у кого исходящий баланс последнего биллинга дебетовый
        )
, hist as ( /*построение истории возникновения дебета а так же начисления каждого месяца с момента возникновения дебета*/
            select * from (select rank() over(partition by clnt_clnt_id order by is_debt desc) rnc
                     , r.*
                  from (select d.clnt_clnt_id, d.in_debt, d.pays pays_n, trunc(b.bill_date,'mm') bill_mnth
                     , min(b.old_balance_$) keep(dense_rank first order by b.bill_date) in_bal
                     , min(b.new_balance_$) keep(dense_rank first order by b.bill_date desc) out_bal
                     , sign(min(b.new_balance_$) keep(dense_rank first order by b.bill_date desc)) is_debt
                     , sum(b.summa_all_$) chrg--, sum(b.payments_all_$+b.adjust_pay_$) pays
                  from debs d
                  left join bis.bills b on b.clnt_clnt_id = d.clnt_clnt_id
                       and nvl(b.bltp_bltp_id,0) = 0
                       and b.bill_date <= d_bill_date
                  group by d.clnt_clnt_id, d.in_debt, d.pays, trunc(b.bill_date,'mm')) r)
            where rnc = 1)
, hist_r as (/*выстраиваем историю группируя по удаленности месяцов*/
            select clnt_clnt_id, mnth, sum(chrg) chrg--, sum(pays) pays
            from (select months_between(trunc(:mnth_end,'mm'),bill_mnth) mnth_dif
                     ,decode(months_between(trunc(:mnth_end,'mm'),bill_mnth)
                            , 0, 0--'0-30'
                            , 1, 1--'30-60'
                            , 2, 2--'60-90'
                            , 3--'90+'
                     ) mnth
                     ,hist.*
                 from hist) f
             group by clnt_clnt_id, mnth)
, forrecur as (/*подготовка полученных данных для рекурсивной обработки*/
        select d.*,nvl(h.chrg,0) chrg
        ,nvl(h.mnth,0) mnth
        ,decode(sign(d.pays - d.in_debt),-1,0,(d.pays - d.in_debt)) prepay
        ,ROW_NUMBER() over(partition by d.clnt_clnt_id order by nvl(h.mnth,0)) row_n
        from debs d
        left join hist_r h on h.clnt_clnt_id = d.clnt_clnt_id
    )
, 
/*рекурсивная обработка полученных данных,
 рекурсивно проходим по всем месяцам раскидывая дебет согласно начислений и платежи периода неучтенного в биллинге*/
recur(clnt_clnt_id,in_debt,pays,d_bill_date,prepay,row_n,new_debit,new_pays, chrg_R,pays_r,chrg,mnth)
 as (
    select f.clnt_clnt_id
         , f.in_debt
         , f.pays
         , f.d_bill_date
     , f.prepay
         , f.row_n
         , decode(sign(f.in_debt - decode(sign(f.chrg),-1,0,f.chrg)),-1,0,f.in_debt - decode(sign(f.chrg),-1,0,f.chrg)
         )new_debit
         , decode(sign(f.pays - decode(sign(f.chrg),-1,0,f.chrg)),-1,0,f.pays - decode(sign(f.chrg),-1,0,f.chrg)
         ) new_pays
         , decode(sign(f.in_debt - decode(sign(f.chrg),-1,0,f.chrg)),-1,f.in_debt, decode(sign(f.chrg),-1,0,f.chrg)
         ) chrg_R
         , decode(sign(f.pays - decode(sign(f.in_debt - decode(sign(f.chrg),-1,0,f.chrg)),-1,f.in_debt, decode(sign(f.chrg),-1,0,f.chrg))),-1,f.pays
                  ,decode(sign(f.in_debt - decode(sign(f.chrg),-1,0,f.chrg)),-1,f.in_debt, decode(sign(f.chrg),-1,0,f.chrg))
         ) pays_r
         ,f.chrg
         ,f.mnth
    from forrecur f
    where f.row_n=1
    union all
    select f.clnt_clnt_id
       , f.in_debt
       , f.pays
       , f.d_bill_date
     , f.prepay
         , f.row_n
         , decode(sign(s.new_debit - decode(sign(f.chrg),-1,0,f.chrg)),-1,0,s.new_debit - decode(sign(f.chrg),-1,0,f.chrg)
         )  new_debit
         , decode(sign(s.new_pays - decode(sign(f.chrg),-1,0,f.chrg)),-1,0,s.new_pays - decode(sign(f.chrg),-1,0,f.chrg)
         ) new_pays
         , decode(sign(s.new_debit - decode(sign(f.chrg),-1,0,f.chrg)),-1,s.new_debit, decode(sign(f.chrg),-1,0,f.chrg)
         )  chrg_R
         , decode(sign(s.new_pays - decode(sign(s.new_debit - decode(sign(f.chrg),-1,0,f.chrg)),-1,s.new_debit, decode(sign(f.chrg),-1,0,f.chrg))),-1,s.new_pays
           ,decode(sign(s.new_debit - decode(sign(f.chrg),-1,0,f.chrg)),-1,s.new_debit, decode(sign(f.chrg),-1,0,f.chrg))
         ) pays_r
         ,f.chrg
         ,f.mnth
    from forrecur f
    join recur s on s.clnt_clnt_id=f.clnt_clnt_id
      and f.row_n=s.row_n+1
    where s.new_debit>0
)
,rez as( /*формирование итоговых данных*/
     select
        f.clnt_clnt_id
        , f.in_debt
        , f.pays
        , f.d_bill_date
        , f.prepay
        /*распределение начислений и платежей по периодам/колонкам */
        ,sum(case when f.row_n=1 then f.chrg_R else 0 end) chrg0
        ,sum(case when f.row_n=1 then f.pays_r else 0 end) pays0
        ,sum(case when f.row_n=2 then f.chrg_R else 0 end) chrg30
        ,sum(case when f.row_n=2 then f.pays_r else 0 end) pays30
        ,sum(case when f.row_n=3 then f.chrg_R else 0 end) chrg60
        ,sum(case when f.row_n=3 then f.pays_r else 0 end) pays60
        ,sum(case when f.row_n=4 then f.chrg_R else 0 end) chrg90
        ,sum(case when f.row_n=4 then f.pays_r else 0 end) pays90
    from recur f
    group by f.clnt_clnt_id
        , f.in_debt
        , f.pays
        , f.d_bill_date
        , f.prepay
)
select obl,reg,
       def,kat, count(account) kol_account, sum(in_debt)in_debt, sum(pays)pays,  sum(prepay)prepay,
       sum(chrg0)chrg0,sum(pays0)pays0,sum(chrg30)chrg30,sum(pays30)pays30,sum(chrg60)chrg60,sum(pays60)pays60, sum(chrg90)chrg90, sum(pays90)pays90
from(
select bg.def obl,br.name reg,ch.account, ch.name, cs.def, ct.def kat, rez.clnt_clnt_Id,rez.in_debt,rez.pays,rez.d_bill_date,rez.prepay
,rez.chrg0,rez.pays0,rez.chrg30,rez.pays30,rez.chrg60,rez.pays60
/* CLM-355787,rez.in_debt-rez.chrg0-rez.chrg30-rez.chrg60-rez.chrg90+rez.chrg90 chrg90
,rez.pays90*/
,decode(sign(rez.in_debt-rez.chrg0-rez.chrg30-rez.chrg60),-1,0,rez.in_debt-rez.chrg0-rez.chrg30-rez.chrg60) chrg90         
,decode(sign(rez.prepay),1,rez.in_debt-rez.pays0-rez.pays30-rez.pays60, decode(sign(rez.pays - rez.pays0 - rez.pays30 - rez.pays60),-1,0,rez.pays - rez.pays0 - rez.pays30 - rez.pays60))   pays90    
from rez
join bis.client_histories ch
     on ch.clnt_clnt_id = rez.clnt_clnt_id
     and ch.start_date <= :mnth_end                
     and ch.end_date > :mnth_end   
join bis.client_classes cs on cs.clcl_id = ch.clcl_clcl_id
join bis.client_types ct on ct.ctyp_id = ch.ctyp_ctyp_id
join bis.branches br on br.brnc_id = ch.brnc_brnc_id
join bis.branches_groups bg on bg.brgr_id = br.brgr_brgr_id 
where 0=0
)
group by (obl,reg,def,kat )
order by 1,2,3



