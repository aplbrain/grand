# What about Neptune?

Graph algorithms are notoriously hard to profile, and predicting workload for arbitrary data science manipulations on a graph is a major challenge in many big-data graph or network-science applications.

[AWS Neptune](https://aws.amazon.com/neptune/getting-started/) is a graph database as a service provided by Amazon Web Services. Though it says "serverless" on the packaging, there are a few considerations to be aware of:

* Neptune requires that you provision a "server-equivalent" amount of compute power. For example, you can provision a single EC2 instance's worth of compute (in which case it is equivalent to running a graph database on a single node for writes. ([Reads can be parallelized across a provisioned cluster.](https://docs.aws.amazon.com/neptune/latest/userguide/intro.html))
* Unlike DynamoDB, Neptune does not have an on-demand auto-scaling feature. In other words, if you rapidly double the number of queries you're running per second in DynamoDB, it meets your need. If you rapidly double the number of queries you send to Neptune, it may very likely fail. [[DynamoDB Auto-Scaling Documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AutoScaling.html)]

This means for certain sparse or bursty use-cases, Neptune may be _dramatically_ more expensive. For example, consider these cases:

### Neptune's pricing model excels:

* Homogenous workload, consistent algorithmic complexity at all hours 
* Adiabatic (gradual) changes to server load that take place over many minutes or hours (to give auto-scaling software time to adapt)

### Neptune's pricing model may be disadvantageous:
* Every hour, new pharmaceutical preprints are crawled and added to a graph
* A user can trigger an arbitrary graph database algorithm via API

As a workaround, Grand rewrites graph operations in an abstracted graph API representation, and then implements these calls as operations on a DynamoDB table. This is LESS EFFICIENT than using a graph database like Neptune, but it frees the user to pay only for the compute and resources that they are using.

## What is the long-term fix?

Neptune is still a relatively young product, and I'm hopeful that AWS will consider adding "true-serverless" pricing models, as they have done with "on-demand" DynamoDB pricing and, more recently, Aurora (though [this is still in preview](https://pages.awscloud.com/AmazonAuroraServerlessv2Preview.html)).

In the medium-term, it is possible to engineer your own scaling software for Neptune that listens for traffic and scales up to meet demand. This capability is currently out of scope for the Grand library.

---

To read a dramatic screenplay retelling of this document, click [here](What-About-Neptune-Dramatic-Retelling.md).
