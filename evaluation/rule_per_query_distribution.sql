-- retrieve for every query the number of effective, alternative, and required rules
select q.query_path,
       (
           select count(distinct qeo.optimizer)
           from query_effective_optimizers qeo
           where qeo.query_id = q.id and not exists (
            select *
            from query_effective_optimizers_dependencies qeod
            where qeod.query_id = qeo.query_id and qeod.dependent_optimizer = qeo.optimizer
           )
       ) as effective_rules,
       (
           select count(distinct qro.optimizer)
           from query_required_optimizers qro
           where qro.query_id = q.id
       ) as required_rules,
       (
           select count(distinct qeod.optimizer)
           from query_effective_optimizers_dependencies qeod
           where qeod.query_id = q.id
       ) as alternative_rules
from queries q;
