import asyncio
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.errors import BadRequest
from pyrogram.types import Message
from sqlalchemy import Boolean, Column, DateTime, Enum, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker


TEXT_1 = 'Текст1'
TEXT_2 = 'Текст2'
TEXT_3 = 'Текст3'
TRIGGER = 'стоп'

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)


app = Client(
    'voronka',
    os.getenv('api_id'),
    os.getenv('api_hash'),
    bot_token=os.getenv('bot_token')
)


Base = declarative_base()

engine = create_async_engine(os.getenv('database_url'))

Session = sessionmaker(class_=AsyncSession, bind=engine)


class User(Base):
    __tablename__ = 'users'

    id = Column(String, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(
        Enum('alive', 'dead', 'finished', name='status_enum'),
        default='alive'
    )
    status_updated_at = Column(DateTime, default=datetime.utcnow)
    message_sending = Column(Integer, default=0)
    status_message_sending = Column(Boolean, default=False)
    date_message_sending = Column(DateTime)
    status_trigger = Column(Boolean, default=False)
    date_status_trigger = Column(DateTime)
    date_first_message = Column(DateTime)


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def send_message(user: User, text: str, message_sending: int) -> User:
    try:
        await app.send_message(user.id, text)
        user.date_message_sending = datetime.utcnow()
        user.message_sending = message_sending
        user.status_message_sending = False
        logging.info("Сообщение №%s отправлено", message_sending)
        return user
    except BadRequest:
        user.status = 'dead'
        user.status_updated_at = datetime.utcnow()
        user.status_message_sending = False
        logging.info("Пользователь №%s переведен в статус 'dead'", user.id)
        return user


async def finish(user: User) -> User:
    user.status = 'finished'
    user.status_updated_at = datetime.utcnow()
    logging.info("Для пользователя №%s воронка окончена", user.id)
    return user


@app.on_message(filters.command('start'))
async def start(client: Client, message: Message) -> None:
    async with Session() as session:
        user = await session.execute(
            select(User)
            .filter(User.id == str(message.from_user.id))
        )
        user = user.scalar()
        if user is None:
            user = User(id=str(message.from_user.id))
            session.add(user)
            await session.commit()
            await session.refresh(user)
            logging.info("Добавлен пользовател №%s", user.id)
        await message.reply('Привет! Я бот Pyrogram.')


@app.on_message(filters.private & ~filters.me)
async def process_message(client: Client, message: Message) -> None:
    try:
        async with Session() as session:
            user = await session.execute(
                select(User)
                .filter(User.id == str(message.from_user.id))
            )
            user = user.scalar()

            if (
                    user.status == 'alive'
                    and user.message_sending == 0
                    and user.status_trigger is False
                    and not user.date_first_message
                    and user.status_message_sending is False
            ):
                user.date_first_message = datetime.utcnow()
                await session.commit()
                await session.refresh(user)

                if ('прекрасно' not in TEXT_1.lower()
                        and 'ожидать' not in TEXT_1.lower()):
                    user.status_message_sending = True
                    await session.commit()
                    await session.refresh(user)
                    await asyncio.sleep(10 - (datetime.utcnow() - user.date_first_message).total_seconds())
                    # await asyncio.sleep(360 - (datetime.utcnow() - user.date_first_message).total_seconds())
                    user = await send_message(user, TEXT_1, 1)

                else:
                    user = await finish(user)
                await session.commit()
                await session.refresh(user)

            if TRIGGER.lower() in TEXT_2.lower():
                user.status_trigger = True
                user.date_status_trigger = datetime.utcnow()
                await session.commit()
                await session.refresh(user)
                logging.info("Для пользователя №%s получен триггер", user.id)

            if (
                    user.status == 'alive'
                    and user.message_sending == 1
                    and user.status_trigger is False
                    and user.status_message_sending is False
            ):
                # if TRIGGER.lower() in TEXT_2.lower():
                #     user.status_trigger = True
                #     user.date_status_trigger = datetime.utcnow()
                #     await session.commit()
                #     await session.refresh(user)
                    # break
                if (
                        'прекрасно' not in TEXT_2.lower()
                        and 'ожидать' not in TEXT_2.lower()
                ):
                    user.status_message_sending = True
                    await session.commit()
                    await session.refresh(user)
                    # await asyncio.sleep(2340 - (datetime.utcnow() - user.date_message_sending).total_seconds())
                    await asyncio.sleep(10 - (datetime.utcnow() - user.date_message_sending).total_seconds())
                    user = await send_message(user, TEXT_2, 2)

                else:
                    user = await finish(user)
                await session.commit()
                await session.refresh(user)

            if (
                    user.status == 'alive'
                    and (user.message_sending == 2 or user.message_sending == 1)
                    and user.status_message_sending is False
            ):
                if (
                        'прекрасно' not in TEXT_3.lower()
                        and 'ожидать' not in TEXT_3.lower()
                ):
                    user.status_message_sending = True
                    await session.commit()
                    await session.refresh(user)
                    timesleep = (
                            datetime.utcnow() - user.date_status_trigger).total_seconds() if user.status_trigger else (
                            datetime.utcnow() - user.date_message_sending).total_seconds()
                    # await asyncio.sleep(93600 - timesleep)
                    await asyncio.sleep(10 - timesleep)
                    user = await send_message(user, TEXT_3, 3)
                else:
                    user = await finish(user)
                await session.commit()
                await session.refresh(user)

    except:
        await session.rollback()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await app.start()
    await asyncio.Event().wait()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
