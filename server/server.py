#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import datetime
import os
import base64
import time
import shutil

from sqlalchemy import create_engine, Column, Integer, Float, Boolean, String, LargeBinary, DateTime, Interval
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.schema import ForeignKey
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

if __name__ == '__main__':
    # Change dir to script dir, see https://stackoverflow.com/a/1432949/807307
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

db = create_engine('sqlite:///db.sqlite', echo=False)
Session = sessionmaker(db)
Base = declarative_base(db)

class JobType(Base):
    __tablename__ = 'job_types'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

class QueuedJob(Base):
    __tablename__ = 'queued_jobs'
    __table_args__ = {'sqlite_autoincrement': True}

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

def log_error(msg):
    with open('log.txt', 'a') as flog:
        flog.write(str(datetime.datetime.now()) + ': ERROR! ' + msg + '\n')

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
        job.timeouts_on = now + datetime.timedelta(hours=24)
        log('log {} is assigned to {}'.format(job.id, job.assigned_to))
    return job

def find_job(session, job_id):
    return session.query(QueuedJob).filter(QueuedJob.id == job_id).first()

def give_back_job(session, job):
    job.assigned_to = None
    job.assigned_on = None
    job.timeouts_on = None
    log('job {} is given back and deassigned'.format(job.id))

def finish_job(session, job):
    session.delete(job)
    log('job {} has finished successfully and is deleted'.format(job.id))

def stream_script(fout, fin, data, job_id):
    fout.write(b'cat <<end_of_file | base64 -d | tar xz\n')
    base64.encode(fin, fout)
    fout.write(b'end_of_file\n')
    fout.write(b'cat <<end_of_file | base64 -d > data\n')
    fout.write(base64.b64encode(data))
    fout.write(b'\nend_of_file\n')
    fout.write(b'echo ' + str(job_id).encode('utf-8') + b' > id\n')
    fout.write(b'./exec\n')

def client_main(argv):
    try:
        origin = argv[0]
        if origin == '':
            raise IndexError
    except IndexError:
        fail("Could not obtain origin, please fix configuration")

    try:
        command = os.environ["SSH_ORIGINAL_COMMAND"].split(' ')
    except KeyError:
        fail("Please provide a client command")

    if command[0] == 'request-work':
        session = Session()
        timeout_jobs(session)
        session.commit()

        while True:
            job = dequeue_job(session, origin)
            if job is not None:
                try:
                    with open('job_types/{}.tar.gz'.format(job.type.name), 'rb') as fin:
                        stream_script(sys.stdout.buffer, fin, job.data, job.id)
                except FileNotFoundError:
                    log_error('Script for job type "{}" does not exist'.format(job.type.name))
                    fail("Internal error")
                session.commit()
                return
            session.commit()
            time.sleep(60)

    elif command[0] == 'give-back-work':
        fail("Not implemented")

    elif command[0] == 'report-work':
        try:
            job_id = int(command[1])
        except IndexError:
            fail("Please specify id")
        except ValueError:
            fail("Please specify valid id")

        session = Session()
        job = find_job(session, job_id)
        if job is None:
            fail("Invalid job id")
        #if job.assigned_to != origin:
        #    fail("Job assigned to another worker")
        # Terminate transaction while we save reported data, so that
        # the database is not blocked
        session.rollback()

        with open('reports/report_{}.tar.gz'.format(job_id), 'wb') as fout:
            shutil.copyfileobj(sys.stdin.buffer, fout)

        job = find_job(session, job_id)
        if job is None:
            fail("Invalid job id")
        #if job.assigned_to != origin:
        #    fail("Job assigned to another worker")
        finish_job(session, job)
        session.commit()

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
