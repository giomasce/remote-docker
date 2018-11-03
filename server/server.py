#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import datetime
import os

from sqlalchemy import create_engine, Column, Integer, Float, Boolean, String, LargeBinary, DateTime, Interval
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.schema import ForeignKey
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

db = create_engine('sqlite:///db.sqlite', echo=True)
Session = sessionmaker(db)
Base = declarative_base(db)

class JobType(Base):
    __tablename__ = 'job_types'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

class QueuedJob(Base):
    __tablename__ = 'queued_jobs'

    id = Column(Integer, primary_key=True)
    type_id = Column(Integer, ForeignKey(JobType.id, onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
    data = Column(LargeBinary, nullable=False)
    created_on = Column(DateTime, nullable=False)
    assigned_to = Column(String, nullable=True)
    assigned_on = Column(DateTime, nullable=True)
    timeouts_on = Column(DateTime, nullable=True)

    type = relationship(JobType)

# class LogLine(Base):
#     __tablename__ = 'log_lines'

#     id = Column(Integer, primary_key=True)
#     datetime = Column(DateTime, nullable=False)
#     action = Column(String, nullable=False)

Base.metadata.create_all(db)

def fail(msg):
    print(msg)
    sys.exit(1)

def log(msg):
    with open('log.txt', 'a') as flog:
        flog.write(str(datetime.datetime.now()) + ': ' + msg + '\n')

def timeout_jobs(session):
    now = datetime.datetime.now()
    for job in session.query(QueuedJob).filter(QueuedJob.timeouts_on <= now):
        job.assigned_to = None
        job.assigned_on = None
        job.timeouts_on = None
        log('job {} timed out and is deassigned'.format(job.id))

def dequeue_job(session, origin):
    now = datetime.datetime.now()
    job = session.query(QueuedJob).filter(QueuedJob.assigned_to == None).order_by(QueuedJob.created_on).first()
    if job is not None:
        job.assigned_to = origin
        job.assigned_on = now
        job.timeouts_on = now + datetime.timedelta(hours=6)
        log('log {} is assigned to {}'.format(job.id, assigned_on))
    return job

def give_back_job(session, job):
    job.assigned_to = None
    job.assigned_on = None
    job.timeouts_on = None
    log('job {} is given back and deassigned'.format(job.id))

def finish_job(session, job):
    session.delete(job)
    log('job {} has finished successfully and is deleted'.format(job.id))

def client_main(argv):
    try:
        origin = argv[0]
        if origin == '':
            raise IndexError
    except IndexError:
        fail("Could not obtain origin, please fix configuration")

    try:
        command = os.environ["SSH_ORIGINAL_COMMAND"]
    except KeyError:
        fail("Please provide a client command")

    if command == 'request-work':
        session = Session()
        timeout_jobs(session)
        session.commit()

    elif command == 'give-back-work':
        pass

    elif command == 'report-work':
        pass

    else:
        fail("Comman unknown")

def server_main(argv):
    try:
        command = argv[0]
    except IndexError:
        fail("Please provide a command")

    if command == 'add-job-type':
        job_type = JobType()
        try:
            job_type_name = argv[1]
        except IndexError:
            fail("Please provide job type name")
        job_type.name = job_type_name

        try:
            session = Session()
            session.add(job_type)
            session.commit()
        except IntegrityError:
            fail("Job type with specified name already exists")

        log('create job type "{}"'.format(job_type_name))

    elif command == 'queue-job':
        job = QueuedJob()
        try:
            job_type_name = argv[1]
            job.data = argv[2].encode('utf-8')
        except IndexError:
            fail("Please provide job type name and job data")

        job.created_on = datetime.datetime.now()

        session = Session()
        try:
            job.type = session.query(JobType).filter(JobType.name == job_type_name).one()
        except NoResultFound:
            fail("Job type does not exist")
        session.add(job)
        session.commit()

        log('queue job {} of type "{}"'.format(job.id, job_type_name))

    else:
        fail("Command unknown")

def main():
    try:
        command = sys.argv[1]
    except IndexError:
        fail("Please provide a command")

    if command == 'client-command':
        client_main(sys.argv[2:])
    else:
        server_main(sys.argv[1:])

if __name__ == '__main__':
    main()
